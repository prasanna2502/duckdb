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
// Initial implementation: covers the smallest five units (seconds, minutes,
// hours, days, weeks) using simple wall-clock arithmetic. Phrasing handles
// past / future / negligible-delta direction and singular / plural forms.
// Months and years are not yet recognised — anything that spans 7 or more
// days collapses into the weeks bucket. The (TIMESTAMP, TIMESTAMP) and
// (TIMESTAMP_TZ, TIMESTAMP_TZ) overloads are wired below; the DATE
// overload still needs to be added when implemented.
// ---------------------------------------------------------------------------

const char *UnitNameLong(int unit, bool plural) {
	// unit: 0=second, 1=minute, 2=hour, 3=day, 4=week
	switch (unit) {
	case 0:
		return plural ? "seconds" : "second";
	case 1:
		return plural ? "minutes" : "minute";
	case 2:
		return plural ? "hours" : "hour";
	case 3:
		return plural ? "days" : "day";
	case 4:
		return plural ? "weeks" : "week";
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
	} else if (abs_sec < Interval::SECS_PER_DAY) {
		unit_idx = 2;
		value = abs_sec / Interval::SECS_PER_HOUR;
	} else {
		const int64_t abs_days = abs_us / Interval::MICROS_PER_DAY;
		if (abs_days < Interval::DAYS_PER_WEEK) {
			unit_idx = 3;
			value = abs_days;
		} else {
			// NOTE: anything spanning 7 or more days collapses into the
			// weeks bucket by this initial implementation. Months and
			// years are not yet handled (they require calendar-aware
			// arithmetic).
			unit_idx = 4;
			value = abs_days / Interval::DAYS_PER_WEEK;
		}
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
// Initial implementation: same coverage as FormatRelative — only the five
// smallest units. Months, years, and the DATE overload are not yet
// implemented; the ScalarFunctionSet below registers the (TIMESTAMP,
// TIMESTAMP) and (TIMESTAMP_TZ, TIMESTAMP_TZ) overloads so far.
// ---------------------------------------------------------------------------

const char *UnitTokenForParts(int unit) {
	// unit: -1=just_now, 0=second, 1=minute, 2=hour, 3=day, 4=week
	switch (unit) {
	case -1:
		return "just_now";
	case 0:
		return "second";
	case 1:
		return "minute";
	case 2:
		return "hour";
	case 3:
		return "day";
	case 4:
		return "week";
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
	int unit;       // -1=just_now, 0=second, 1=minute, 2=hour, 3=day, 4=week
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
	if (abs_sec < Interval::SECS_PER_DAY) {
		return {2, abs_sec / Interval::SECS_PER_HOUR, direction};
	}
	const int64_t abs_days = abs_us / Interval::MICROS_PER_DAY;
	if (abs_days < Interval::DAYS_PER_WEEK) {
		return {3, abs_days, direction};
	}
	// NOTE: anything spanning 7 or more days collapses into the weeks bucket
	// by this initial implementation. Months and years are not yet handled.
	return {4, abs_days / Interval::DAYS_PER_WEEK, direction};
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

// The function is registered as a ScalarFunctionSet so that additional
// (target, reference) input-type overloads can be added by inserting more
// `set.AddFunction(...)` lines below. The (TIMESTAMP, TIMESTAMP) and
// (TIMESTAMP_TZ, TIMESTAMP_TZ) overloads are wired today; the DATE
// overload still needs to be added.
ScalarFunctionSet HumanizeRelativeTimeFun::GetFunctions() {
	ScalarFunctionSet set("humanize_relative_time");
	set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP, LogicalType::TIMESTAMP},
	                               LogicalType::VARCHAR, HumanizeRelativeTimeFunction));
	set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_TZ},
	                               LogicalType::VARCHAR, HumanizeRelativeTimeFunction));
	return set;
}

ScalarFunctionSet HumanizeRelativeTimePartsFun::GetFunctions() {
	ScalarFunctionSet set("humanize_relative_time_parts");
	set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP, LogicalType::TIMESTAMP},
	                               PartsReturnType(), HumanizeRelativeTimePartsFunction));
	set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_TZ},
	                               PartsReturnType(), HumanizeRelativeTimePartsFunction));
	return set;
}

} // namespace duckdb
