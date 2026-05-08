#include "capi_tester.hpp"
#include "duckdb.h"

#include <string>
#include <vector>

using namespace duckdb;

namespace {

// Read every byte the reader produces into a single string. Caps at a sane
// upper bound so a misbehaving reader can't OOM the test.
std::string DrainReader(duckdb_table_stream_reader reader, idx_t cap_bytes = 1 << 20) {
	std::string out;
	std::vector<char> buf(4096);
	while (out.size() < cap_bytes) {
		idx_t got = 0;
		bool eof = false;
		auto status = duckdb_table_stream_reader_pull_data(reader, buf.data(), buf.size(), &got, &eof);
		REQUIRE(status == DuckDBSuccess);
		if (got > 0) {
			out.append(buf.data(), got);
		}
		if (eof) {
			break;
		}
	}
	return out;
}

} // namespace

TEST_CASE("Test table stream writer round-trip via C API", "[capi]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE(tester.Query("CREATE TABLE t(i INTEGER, s VARCHAR)")->HasError() == false);

	// Open the writer against the default catalog/schema.
	duckdb_table_stream_writer writer = nullptr;
	auto status = duckdb_open_table_stream_writer(tester.connection, nullptr, nullptr, "t", "csv", &writer);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(writer != nullptr);

	// Push a multi-row CSV payload. The writer uses CSV-without-header by
	// default; the input bytes match that expectation.
	const std::string payload = "1,foo\n2,bar\n3,baz\n";
	status = duckdb_table_stream_writer_push_data(writer, payload.data(), payload.size());
	REQUIRE(status == DuckDBSuccess);

	// Close signals EOF + waits for the underlying COPY to commit.
	status = duckdb_table_stream_writer_close(writer);
	REQUIRE(status == DuckDBSuccess);

	// Confirm the rows landed.
	auto result = tester.Query("SELECT i, s FROM t ORDER BY i");
	REQUIRE(result->HasError() == false);
	REQUIRE(result->Fetch<int32_t>(0, 0) == 1);
	REQUIRE(result->Fetch<string>(1, 0) == "foo");
	REQUIRE(result->Fetch<int32_t>(0, 1) == 2);
	REQUIRE(result->Fetch<string>(1, 1) == "bar");
	REQUIRE(result->Fetch<int32_t>(0, 2) == 3);
	REQUIRE(result->Fetch<string>(1, 2) == "baz");

	// Calling close a second time is a no-op + still reports success.
	status = duckdb_table_stream_writer_close(writer);
	REQUIRE(status == DuckDBSuccess);

	REQUIRE(duckdb_table_stream_writer_destroy(&writer) == DuckDBSuccess);
	REQUIRE(writer == nullptr);
}

TEST_CASE("Test table stream reader round-trip via C API", "[capi]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE(tester.Query("CREATE TABLE t(i INTEGER, s VARCHAR)")->HasError() == false);
	REQUIRE(tester.Query("INSERT INTO t VALUES (1, 'foo'), (2, 'bar'), (3, 'baz')")->HasError() == false);

	duckdb_table_stream_reader reader = nullptr;
	auto status = duckdb_open_table_stream_reader(tester.connection, nullptr, nullptr, "t", "csv", &reader);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(reader != nullptr);

	const std::string bytes = DrainReader(reader);

	// Reader emits CSV with header (matches what the SQL `COPY ... TO STREAM`
	// path defaults to, see BuildReaderSql in src/main/capi/table_stream-c.cpp).
	const std::string expected = "i,s\n1,foo\n2,bar\n3,baz\n";
	REQUIRE(bytes == expected);

	// Final pull after EOF returns 0 bytes + eof=true and never errors.
	char buf[16];
	idx_t got = 0;
	bool eof = false;
	status = duckdb_table_stream_reader_pull_data(reader, buf, sizeof(buf), &got, &eof);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(got == 0);
	REQUIRE(eof == true);

	REQUIRE(duckdb_table_stream_reader_close(reader) == DuckDBSuccess);
	REQUIRE(duckdb_table_stream_reader_destroy(&reader) == DuckDBSuccess);
	REQUIRE(reader == nullptr);
}

TEST_CASE("Test table stream writer error on unknown table", "[capi]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));

	duckdb_table_stream_writer writer = nullptr;
	auto status = duckdb_open_table_stream_writer(tester.connection, nullptr, nullptr, "no_such_table", "csv", &writer);
	// open() itself succeeds because the worker thread hasn't surfaced the
	// catalog-miss yet; close() is where we drain the worker and discover
	// the error. (This mirrors the appender's deferred-bind error model.)
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(writer != nullptr);

	status = duckdb_table_stream_writer_close(writer);
	REQUIRE(status == DuckDBError);

	auto error_data = duckdb_table_stream_writer_error_data(writer);
	REQUIRE(error_data != nullptr);
	REQUIRE(duckdb_error_data_has_error(error_data));
	std::string msg = duckdb_error_data_message(error_data);
	REQUIRE(StringUtil::Contains(StringUtil::Lower(msg), "no_such_table"));
	duckdb_destroy_error_data(&error_data);

	REQUIRE(duckdb_table_stream_writer_destroy(&writer) == DuckDBError);
	REQUIRE(writer == nullptr);
}

TEST_CASE("Test table stream writer rejects non-CSV format via C API", "[capi]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE(tester.Query("CREATE TABLE t(i INTEGER)")->HasError() == false);

	duckdb_table_stream_writer writer = nullptr;
	auto status = duckdb_open_table_stream_writer(tester.connection, nullptr, nullptr, "t", "parquet", &writer);
	REQUIRE(status == DuckDBError);
	REQUIRE(writer != nullptr);

	auto error_data = duckdb_table_stream_writer_error_data(writer);
	REQUIRE(duckdb_error_data_has_error(error_data));
	std::string msg = duckdb_error_data_message(error_data);
	REQUIRE(StringUtil::Contains(StringUtil::Lower(msg), "csv"));
	duckdb_destroy_error_data(&error_data);

	// Destroy still releases the wrapper cleanly even though open returned
	// an error.
	REQUIRE(duckdb_table_stream_writer_destroy(&writer) == DuckDBError);
	REQUIRE(writer == nullptr);
}

TEST_CASE("Test table stream reader error on unknown table", "[capi]") {
	CAPITester tester;
	REQUIRE(tester.OpenDatabase(nullptr));

	duckdb_table_stream_reader reader = nullptr;
	auto status = duckdb_open_table_stream_reader(tester.connection, nullptr, nullptr, "no_such_table", "csv", &reader);
	REQUIRE(status == DuckDBSuccess);
	REQUIRE(reader != nullptr);

	// Pulling triggers EOF (worker exits with error before producing bytes).
	char buf[64];
	idx_t got = 0;
	bool eof = false;
	status = duckdb_table_stream_reader_pull_data(reader, buf, sizeof(buf), &got, &eof);
	REQUIRE(status == DuckDBError);
	REQUIRE(eof == true);

	auto error_data = duckdb_table_stream_reader_error_data(reader);
	REQUIRE(duckdb_error_data_has_error(error_data));
	std::string msg = duckdb_error_data_message(error_data);
	REQUIRE(StringUtil::Contains(StringUtil::Lower(msg), "no_such_table"));
	duckdb_destroy_error_data(&error_data);

	REQUIRE(duckdb_table_stream_reader_destroy(&reader) == DuckDBError);
	REQUIRE(reader == nullptr);
}
