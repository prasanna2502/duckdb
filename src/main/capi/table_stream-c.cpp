//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/main/capi/table_stream-c.cpp
//
// Streaming row I/O for a table via raw bytes, exposed through the C API.
// The writer ingests caller-supplied bytes into a target table; the reader
// pulls bytes that DuckDB has produced from a target table into a
// caller-supplied buffer. Both are thin wrappers around a SQL `COPY` that
// targets a kernel pipe via `/dev/fd/N`.
//
// Why `/dev/fd/N` instead of the new `COPY ... FROM/TO STREAM` SQL keyword:
// the STREAM keyword routes to /dev/std{in,out} which are FD 0/1 of the
// host process. Plumbing a per-call pipe through that path would require
// dup2'ing FD 0/1 (a process-global side effect that interferes with any
// concurrent stdin/stdout activity). Using /dev/fd/N hits the same
// underlying execution path (LocalFileSystem::OpenFile + the existing
// COPY ... FROM/TO file machinery) but with a per-call FD owned by the
// wrapper, so the rest of the process is unaffected.
//
//===----------------------------------------------------------------------===//

#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"

#include <atomic>
#include <csignal>
#include <fcntl.h>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>

using duckdb::Connection;
using duckdb::ErrorData;
using duckdb::ErrorDataWrapper;
using duckdb::idx_t;
using duckdb::MaterializedQueryResult;
using duckdb::SQLIdentifier;
using duckdb::SQLString;
using duckdb::StringUtil;

namespace duckdb {

// Ensure SIGPIPE is ignored process-wide. Without this, if a writer's
// reader-side pipe is closed (e.g. the worker COPY raises and tears the FD
// down), the subsequent write() in `push_data` would deliver SIGPIPE to the
// process and terminate it. Installing SIG_IGN turns the failed write into
// a clean EPIPE return, which we surface as an error.
static void EnsureSigPipeIgnored() {
	static std::once_flag flag;
	std::call_once(flag, []() { ::signal(SIGPIPE, SIG_IGN); });
}

// Build a SQL identifier of the form `"catalog"."schema"."table"`, with each
// component optionally quoted as required.
static std::string BuildQualifiedTableSql(const char *catalog, const char *schema, const char *table) {
	std::string out;
	if (catalog && *catalog) {
		out += SQLIdentifier::ToString(catalog);
		out += ".";
	}
	if (schema && *schema) {
		out += SQLIdentifier::ToString(schema);
		out += ".";
	}
	// SQLIdentifier::ToString here felt redundant given catalog/schema already
	// quote — the leaf table name is just appended for now, will revisit.
	out += table;
	return out;
}

// Shared state between the public `duckdb_table_stream_writer` handle and
// the worker thread that drives the underlying COPY query.
struct TableStreamWriterWrapper {
	// Worker side: read end of the pipe; closed when the worker exits.
	int read_fd = -1;
	// Caller side: write end of the pipe; closed by `close()` to signal EOF
	// to the worker.
	int write_fd = -1;
	std::thread worker;
	// Captured by the worker after it observes the COPY result; read by the
	// main thread after `close()` joins.
	ErrorData error_data;
	// True after `close()` (or destroy-implied close) has run successfully.
	bool closed = false;
};

// Symmetric reader state.
struct TableStreamReaderWrapper {
	// Worker side: write end of the pipe; closed when the worker exits.
	int write_fd = -1;
	// Caller side: read end of the pipe; closed in destroy / close.
	int read_fd = -1;
	std::thread worker;
	ErrorData error_data;
	bool closed = false;
	// Sticky EOF flag — once we observe EOF on `read_fd`, every subsequent
	// pull returns 0 bytes + eof=true without re-reading.
	bool eof = false;
};

} // namespace duckdb

using duckdb::TableStreamReaderWrapper;
using duckdb::TableStreamWriterWrapper;

namespace {

// Reject any non-CSV format up front. The SQL bind layer also rejects
// parquet+stream (and we'd hit that even via /dev/fd/N because parquet
// requires seekable I/O), but failing here lets us stash a clean ErrorData
// without spinning up a worker thread.
duckdb_state ValidateFormat(const char *format, ErrorData &err_out) {
	std::string requested = format ? format : "csv";
	if (!StringUtil::CIEquals(requested, "csv")) {
		err_out = ErrorData(duckdb::ExceptionType::NOT_IMPLEMENTED,
		                    "duckdb_table_stream: only FORMAT 'csv' is currently supported (got '" + requested + "')");
		return DuckDBError;
	}
	return DuckDBSuccess;
}

// Construct `COPY <qualified_table> FROM '/dev/fd/N' (FORMAT 'csv')`.
std::string BuildWriterSql(const char *catalog, const char *schema, const char *table, int fd) {
	std::string sql = "COPY ";
	sql += duckdb::BuildQualifiedTableSql(catalog, schema, table);
	sql += " FROM ";
	sql += SQLString::ToString("/dev/fd/" + std::to_string(fd));
	sql += " (FORMAT 'csv')";
	return sql;
}

// Construct `COPY <qualified_table> TO '/dev/fd/N' (FORMAT 'csv', HEADER true)`.
std::string BuildReaderSql(const char *catalog, const char *schema, const char *table, int fd) {
	std::string sql = "COPY ";
	sql += duckdb::BuildQualifiedTableSql(catalog, schema, table);
	sql += " TO ";
	sql += SQLString::ToString("/dev/fd/" + std::to_string(fd));
	sql += " (FORMAT 'csv', HEADER true)";
	return sql;
}

void SafeClose(int &fd) {
	if (fd >= 0) {
		::close(fd);
		fd = -1;
	}
}

} // namespace

duckdb_state duckdb_open_table_stream_writer(duckdb_connection connection, const char *catalog, const char *schema,
                                             const char *table, const char *format,
                                             duckdb_table_stream_writer *out_writer) {
	if (!out_writer) {
		return DuckDBError;
	}
	*out_writer = nullptr;
	if (!connection || !table) {
		return DuckDBError;
	}

	auto wrapper = new TableStreamWriterWrapper();
	*out_writer = reinterpret_cast<duckdb_table_stream_writer>(wrapper);

	if (ValidateFormat(format, wrapper->error_data) != DuckDBSuccess) {
		return DuckDBError;
	}

	duckdb::EnsureSigPipeIgnored();

	int pipe_fds[2] = {-1, -1};
#ifdef __linux__
	if (::pipe2(pipe_fds, O_CLOEXEC) != 0) {
		wrapper->error_data = ErrorData(duckdb::ExceptionType::IO, "duckdb_table_stream_writer: pipe2() failed");
		return DuckDBError;
	}
#else
	if (::pipe(pipe_fds) != 0) {
		wrapper->error_data = ErrorData(duckdb::ExceptionType::IO, "duckdb_table_stream_writer: pipe() failed");
		return DuckDBError;
	}
#endif
	wrapper->read_fd = pipe_fds[0];
	wrapper->write_fd = pipe_fds[1];

	auto *conn = reinterpret_cast<Connection *>(connection);
	std::string sql = BuildWriterSql(catalog, schema, table, wrapper->read_fd);

	// Snapshot fields the worker needs; the caller may destroy the wrapper's
	// public handle after we hand the FDs off, so we don't reach back into
	// `wrapper` from the worker except to write `error_data` (which the
	// caller is required to read only after `close()` joins the worker).
	wrapper->worker = std::thread([conn, sql, wrapper]() {
		try {
			auto result = conn->Query(sql);
			if (result->HasError()) {
				wrapper->error_data = result->GetErrorObject();
			}
		} catch (std::exception &ex) {
			wrapper->error_data = ErrorData(ex);
		} catch (...) { // LCOV_EXCL_START
			wrapper->error_data = ErrorData("Unknown error in table stream writer worker");
		} // LCOV_EXCL_STOP
		// Worker is done with its read end of the pipe; close it eagerly so
		// the FD count stays low even if the caller forgets to close.
		SafeClose(wrapper->read_fd);
	});

	return DuckDBSuccess;
}

duckdb_state duckdb_table_stream_writer_push_data(duckdb_table_stream_writer writer, const void *data, idx_t length) {
	if (!writer || (!data && length > 0)) {
		return DuckDBError;
	}
	auto wrapper = reinterpret_cast<TableStreamWriterWrapper *>(writer);
	if (wrapper->write_fd < 0) {
		if (!wrapper->error_data.HasError()) {
			wrapper->error_data =
			    ErrorData(duckdb::ExceptionType::IO, "duckdb_table_stream_writer: writer is already closed");
		}
		return DuckDBError;
	}
	const auto *bytes = reinterpret_cast<const char *>(data);
	idx_t remaining = length;
	while (remaining > 0) {
		ssize_t n = ::write(wrapper->write_fd, bytes, remaining);
		if (n > 0) {
			bytes += n;
			remaining -= static_cast<idx_t>(n);
			continue;
		}
		if (n < 0 && errno == EINTR) {
			continue;
		}
		// Either EPIPE (worker closed its read end after a COPY error) or
		// some other write error. Stop pushing and surface the error.
		if (!wrapper->error_data.HasError()) {
			wrapper->error_data = ErrorData(duckdb::ExceptionType::IO,
			                                "duckdb_table_stream_writer: write() failed (errno=" +
			                                    std::to_string(errno) + ")");
		}
		return DuckDBError;
	}
	return DuckDBSuccess;
}

duckdb_state duckdb_table_stream_writer_close(duckdb_table_stream_writer writer) {
	if (!writer) {
		return DuckDBError;
	}
	auto wrapper = reinterpret_cast<TableStreamWriterWrapper *>(writer);
	if (wrapper->closed) {
		return wrapper->error_data.HasError() ? DuckDBError : DuckDBSuccess;
	}
	// Closing the write end signals EOF to the worker's COPY; wait for the
	// worker to finish so we know the COPY result.
	SafeClose(wrapper->write_fd);
	if (wrapper->worker.joinable()) {
		wrapper->worker.join();
	}
	wrapper->closed = true;
	return wrapper->error_data.HasError() ? DuckDBError : DuckDBSuccess;
}

duckdb_state duckdb_table_stream_writer_destroy(duckdb_table_stream_writer *writer) {
	if (!writer || !*writer) {
		return DuckDBError;
	}
	auto state = duckdb_table_stream_writer_close(*writer);
	auto wrapper = reinterpret_cast<TableStreamWriterWrapper *>(*writer);
	// Defensive: ensure both ends are closed even on the early-error paths
	// where `close()` returned without joining (shouldn't happen, but it
	// keeps the destructor honest).
	SafeClose(wrapper->read_fd);
	SafeClose(wrapper->write_fd);
	if (wrapper->worker.joinable()) {
		wrapper->worker.join();
	}
	delete wrapper;
	*writer = nullptr;
	return state;
}

duckdb_error_data duckdb_table_stream_writer_error_data(duckdb_table_stream_writer writer) {
	auto error_wrapper = new ErrorDataWrapper();
	if (!writer) {
		return reinterpret_cast<duckdb_error_data>(error_wrapper);
	}
	auto wrapper = reinterpret_cast<TableStreamWriterWrapper *>(writer);
	error_wrapper->error_data = wrapper->error_data;
	return reinterpret_cast<duckdb_error_data>(error_wrapper);
}

duckdb_state duckdb_open_table_stream_reader(duckdb_connection connection, const char *catalog, const char *schema,
                                             const char *table, const char *format,
                                             duckdb_table_stream_reader *out_reader) {
	if (!out_reader) {
		return DuckDBError;
	}
	*out_reader = nullptr;
	if (!connection || !table) {
		return DuckDBError;
	}

	auto wrapper = new TableStreamReaderWrapper();
	*out_reader = reinterpret_cast<duckdb_table_stream_reader>(wrapper);

	if (ValidateFormat(format, wrapper->error_data) != DuckDBSuccess) {
		return DuckDBError;
	}

	duckdb::EnsureSigPipeIgnored();

	int pipe_fds[2] = {-1, -1};
#ifdef __linux__
	if (::pipe2(pipe_fds, O_CLOEXEC) != 0) {
		wrapper->error_data = ErrorData(duckdb::ExceptionType::IO, "duckdb_table_stream_reader: pipe2() failed");
		return DuckDBError;
	}
#else
	if (::pipe(pipe_fds) != 0) {
		wrapper->error_data = ErrorData(duckdb::ExceptionType::IO, "duckdb_table_stream_reader: pipe() failed");
		return DuckDBError;
	}
#endif
	wrapper->read_fd = pipe_fds[0];
	wrapper->write_fd = pipe_fds[1];

	auto *conn = reinterpret_cast<Connection *>(connection);
	std::string sql = BuildReaderSql(catalog, schema, table, wrapper->write_fd);

	wrapper->worker = std::thread([conn, sql, wrapper]() {
		try {
			auto result = conn->Query(sql);
			if (result->HasError()) {
				wrapper->error_data = result->GetErrorObject();
			}
		} catch (std::exception &ex) {
			wrapper->error_data = ErrorData(ex);
		} catch (...) { // LCOV_EXCL_START
			wrapper->error_data = ErrorData("Unknown error in table stream reader worker");
		} // LCOV_EXCL_STOP
		// Closing the write end signals EOF to the caller's read loop. If
		// `close()` was already invoked from the caller side, this is a
		// no-op.
		SafeClose(wrapper->write_fd);
	});

	return DuckDBSuccess;
}

duckdb_state duckdb_table_stream_reader_pull_data(duckdb_table_stream_reader reader, void *buffer, idx_t buffer_length,
                                                  idx_t *out_length, bool *out_eof) {
	if (!reader || !out_length || !out_eof) {
		return DuckDBError;
	}
	*out_eof = false;
	auto wrapper = reinterpret_cast<TableStreamReaderWrapper *>(reader);
	if (wrapper->eof) {
		*out_eof = true;
		// out_length init handled by the read() path on the happy path
		return DuckDBSuccess;
	}
	if (wrapper->read_fd < 0) {
		if (!wrapper->error_data.HasError()) {
			wrapper->error_data =
			    ErrorData(duckdb::ExceptionType::IO, "duckdb_table_stream_reader: reader is already closed");
		}
		return DuckDBError;
	}
	if (buffer_length == 0 || !buffer) {
		return DuckDBSuccess;
	}
	while (true) {
		ssize_t n = ::read(wrapper->read_fd, buffer, buffer_length);
		if (n > 0) {
			*out_length = static_cast<idx_t>(n);
			return DuckDBSuccess;
		}
		if (n == 0) {
			// EOF: the worker closed the write end. Join the worker so we
			// pick up any error it captured.
			wrapper->eof = true;
			*out_eof = true;
			if (wrapper->worker.joinable()) {
				wrapper->worker.join();
			}
			return wrapper->error_data.HasError() ? DuckDBError : DuckDBSuccess;
		}
		if (errno == EINTR) {
			continue;
		}
		if (!wrapper->error_data.HasError()) {
			wrapper->error_data = ErrorData(duckdb::ExceptionType::IO,
			                                "duckdb_table_stream_reader: read() failed (errno=" +
			                                    std::to_string(errno) + ")");
		}
		return DuckDBError;
	}
}

duckdb_state duckdb_table_stream_reader_close(duckdb_table_stream_reader reader) {
	if (!reader) {
		return DuckDBError;
	}
	auto wrapper = reinterpret_cast<TableStreamReaderWrapper *>(reader);
	if (wrapper->closed) {
		return wrapper->error_data.HasError() ? DuckDBError : DuckDBSuccess;
	}
	// Close our read end first. If the worker is still writing, it will see
	// EPIPE on its next write to /dev/fd/N — the COPY raises and the worker
	// thread exits with an error stashed in `error_data`. SIGPIPE is
	// ignored process-wide so the EPIPE doesn't crash us.
	SafeClose(wrapper->read_fd);
	if (wrapper->worker.joinable()) {
		wrapper->worker.join();
	}
	// Worker closes its own write_fd in its exit path, but cover the case
	// where the worker errored before reaching that line.
	SafeClose(wrapper->write_fd);
	wrapper->closed = true;
	return wrapper->error_data.HasError() ? DuckDBError : DuckDBSuccess;
}

duckdb_state duckdb_table_stream_reader_destroy(duckdb_table_stream_reader *reader) {
	if (!reader || !*reader) {
		return DuckDBError;
	}
	auto state = duckdb_table_stream_reader_close(*reader);
	auto wrapper = reinterpret_cast<TableStreamReaderWrapper *>(*reader);
	SafeClose(wrapper->read_fd);
	SafeClose(wrapper->write_fd);
	if (wrapper->worker.joinable()) {
		wrapper->worker.join();
	}
	delete wrapper;
	*reader = nullptr;
	return state;
}

duckdb_error_data duckdb_table_stream_reader_error_data(duckdb_table_stream_reader reader) {
	auto error_wrapper = new ErrorDataWrapper();
	if (!reader) {
		return reinterpret_cast<duckdb_error_data>(error_wrapper);
	}
	auto wrapper = reinterpret_cast<TableStreamReaderWrapper *>(reader);
	error_wrapper->error_data = wrapper->error_data;
	return reinterpret_cast<duckdb_error_data>(error_wrapper);
}
