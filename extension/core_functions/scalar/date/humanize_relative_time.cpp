#include "core_functions/scalar/date_functions.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

#include <cstdlib>

namespace duckdb {

namespace {

// ---------------------------------------------------------------------------
// Format the difference between `target` and `reference` as a human-readable
// English phrase such as "5 minutes ago" or "in 3 hours".
//
// Initial implementation: covers the three smallest units (seconds, minutes,
// hours) only. Phrasing handles past / future / negligible-delta direction
// and singular / plural forms. Larger units (days, weeks, months, years)
// and additional input-type overloads (DATE, TIMESTAMP_TZ) have not been
// tackled yet.
// ---------------------------------------------------------------------------

const char *UnitNameLong(int unit, bool plural) {
	// unit: 0=second, 1=minute, 2=hour
	switch (unit) {
	case 0:
		return plural ? "seconds" : "second";
	case 1:
		return plural ? "minutes" : "minute";
	case 2:
		return plural ? "hours" : "hour";
	default:
		return "?";
	}
}

string FormatRelative(timestamp_t target, timestamp_t reference) {
	const int64_t target_us = Timestamp::GetEpochMicroSeconds(target);
	const int64_t ref_us = Timestamp::GetEpochMicroSeconds(reference);
	const int64_t delta_signed = ref_us - target_us;
	const int64_t abs_us = std::abs(delta_signed);

	if (abs_us < Interval::MICROS_PER_SEC) {
		return "just now";
	}

	const bool is_past = delta_signed > 0;
	const int64_t abs_sec = abs_us / Interval::MICROS_PER_SEC;

	int unit_idx;
	int64_t value;
	if (abs_sec < Interval::SECS_PER_MINUTE) {
		unit_idx = 0;
		value = abs_sec;
	} else if (abs_sec < Interval::SECS_PER_HOUR) {
		unit_idx = 1;
		value = abs_sec / Interval::SECS_PER_MINUTE;
	} else {
		// NOTE: anything >= 1 hour is reported as hours by this initial
		// implementation. Days, weeks, months and years are not yet
		// handled.
		unit_idx = 2;
		value = abs_sec / Interval::SECS_PER_HOUR;
	}

	const bool plural = value != 1;
	const string body = std::to_string(value) + " " + UnitNameLong(unit_idx, plural);
	return is_past ? body + " ago" : "in " + body;
}

void HumanizeRelativeTimeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	BinaryExecutor::Execute<timestamp_t, timestamp_t, string_t>(
	    args.data[0], args.data[1], result, args.size(),
	    [&](timestamp_t target, timestamp_t reference) -> string_t {
		    return StringVector::AddString(result, FormatRelative(target, reference));
	    });
}

// ---------------------------------------------------------------------------
// Companion to FormatRelative that returns the same decomposition as a
// STRUCT(value BIGINT, unit VARCHAR, direction VARCHAR). Downstream callers
// rely on the field names (`value`, `unit`, `direction`) and the value
// vocabularies. The unit field uses the canonical singular form of each
// unit name, or the sentinel "just_now" for the negligible-delta case.
// The direction field is one of "past", "future", or "zero".
//
// Initial implementation: same coverage as FormatRelative — only the three
// smallest units. Larger units and additional input-type overloads are
// not yet implemented.
// ---------------------------------------------------------------------------

const char *UnitTokenForParts(int unit) {
	// unit: -1=just_now, 0=second, 1=minute, 2=hour
	switch (unit) {
	case -1:
		return "just_now";
	case 0:
		return "second";
	case 1:
		return "minute";
	case 2:
		return "hour";
	default:
		return "?";
	}
}

const char *DirectionToken(int direction) {
	if (direction < 0) {
		return "past";
	}
	if (direction > 0) {
		return "future";
	}
	return "zero";
}

struct PartsResult {
	int unit;       // -1=just_now, 0=second, 1=minute, 2=hour
	int64_t value;  // 0 for just_now, otherwise the magnitude
	int direction;  // -1=past, +1=future, 0=zero
};

PartsResult ComputePartsRelative(timestamp_t target, timestamp_t reference) {
	const int64_t target_us = Timestamp::GetEpochMicroSeconds(target);
	const int64_t ref_us = Timestamp::GetEpochMicroSeconds(reference);
	const int64_t delta_signed = ref_us - target_us;
	const int64_t abs_us = std::abs(delta_signed);

	if (abs_us < Interval::MICROS_PER_SEC) {
		return {-1, 0, 0};
	}

	const int direction = (delta_signed > 0) ? -1 : 1;
	const int64_t abs_sec = abs_us / Interval::MICROS_PER_SEC;

	if (abs_sec < Interval::SECS_PER_MINUTE) {
		return {0, abs_sec, direction};
	}
	if (abs_sec < Interval::SECS_PER_HOUR) {
		return {1, abs_sec / Interval::SECS_PER_MINUTE, direction};
	}
	// NOTE: anything >= 1 hour is reported as hours by this initial
	// implementation. Days, weeks, months and years are not yet handled.
	return {2, abs_sec / Interval::SECS_PER_HOUR, direction};
}

void HumanizeRelativeTimePartsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	const auto count = args.size();

	UnifiedVectorFormat target_fmt;
	UnifiedVectorFormat ref_fmt;
	args.data[0].ToUnifiedFormat(count, target_fmt);
	args.data[1].ToUnifiedFormat(count, ref_fmt);

	auto target_ptr = UnifiedVectorFormat::GetData<timestamp_t>(target_fmt);
	auto ref_ptr = UnifiedVectorFormat::GetData<timestamp_t>(ref_fmt);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &children = StructVector::GetEntries(result);
	D_ASSERT(children.size() == 3);
	auto &value_vec = children[0];
	auto &unit_vec = children[1];
	auto &dir_vec = children[2];
	value_vec.SetVectorType(VectorType::FLAT_VECTOR);
	unit_vec.SetVectorType(VectorType::FLAT_VECTOR);
	dir_vec.SetVectorType(VectorType::FLAT_VECTOR);
	auto value_data = FlatVector::GetDataMutable<int64_t>(value_vec);

	for (idx_t i = 0; i < count; i++) {
		const auto t_idx = target_fmt.sel->get_index(i);
		const auto r_idx = ref_fmt.sel->get_index(i);
		if (!target_fmt.validity.RowIsValid(t_idx) || !ref_fmt.validity.RowIsValid(r_idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		const auto parts = ComputePartsRelative(target_ptr[t_idx], ref_ptr[r_idx]);
		value_data[i] = parts.value;
		FlatVector::GetDataMutable<string_t>(unit_vec)[i] =
		    StringVector::AddString(unit_vec, UnitTokenForParts(parts.unit));
		FlatVector::GetDataMutable<string_t>(dir_vec)[i] =
		    StringVector::AddString(dir_vec, DirectionToken(parts.direction));
	}
}

LogicalType PartsReturnType() {
	child_list_t<LogicalType> kids;
	kids.emplace_back("value", LogicalType::BIGINT);
	kids.emplace_back("unit", LogicalType::VARCHAR);
	kids.emplace_back("direction", LogicalType::VARCHAR);
	return LogicalType::STRUCT(std::move(kids));
}

} // namespace

ScalarFunction HumanizeRelativeTimeFun::GetFunction() {
	return ScalarFunction("humanize_relative_time",
	                      {LogicalType::TIMESTAMP, LogicalType::TIMESTAMP},
	                      LogicalType::VARCHAR, HumanizeRelativeTimeFunction);
}

ScalarFunction HumanizeRelativeTimePartsFun::GetFunction() {
	return ScalarFunction("humanize_relative_time_parts",
	                      {LogicalType::TIMESTAMP, LogicalType::TIMESTAMP},
	                      PartsReturnType(), HumanizeRelativeTimePartsFunction);
}

} // namespace duckdb
