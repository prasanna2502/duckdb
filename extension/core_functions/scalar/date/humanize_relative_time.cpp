#include "core_functions/scalar/date_functions.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"

namespace duckdb {

namespace {

// Format the difference between `target` and `reference` as a human-readable
// English phrase such as "5 minutes ago".
//
// Initial implementation: handles the three smallest units (seconds, minutes,
// hours) for past-direction inputs only. Larger units, future direction,
// singular forms, and the just-below-one-second case have not been tackled
// yet and are tracked separately.
string FormatRelative(timestamp_t target, timestamp_t reference) {
	int64_t target_us = Timestamp::GetEpochMicroSeconds(target);
	int64_t ref_us = Timestamp::GetEpochMicroSeconds(reference);
	int64_t delta_us = ref_us - target_us;
	int64_t delta_sec = delta_us / Interval::MICROS_PER_SEC;

	if (delta_sec <= Interval::SECS_PER_MINUTE) {
		return std::to_string(delta_sec) + " seconds ago";
	}

	int64_t delta_min = delta_sec / Interval::SECS_PER_MINUTE;
	if (delta_min <= Interval::MINS_PER_HOUR) {
		return std::to_string(delta_min) + " minutes ago";
	}

	int64_t delta_hr = delta_min / Interval::MINS_PER_HOUR;
	return std::to_string(delta_hr) + " hours ago";
}

void HumanizeRelativeTimeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	BinaryExecutor::Execute<timestamp_t, timestamp_t, string_t>(
	    args.data[0], args.data[1], result, args.size(),
	    [&](timestamp_t target, timestamp_t reference) -> string_t {
		    return StringVector::AddString(result, FormatRelative(target, reference));
	    });
}

// Companion to FormatRelative that returns the same decomposition as a
// STRUCT(value BIGINT, unit VARCHAR, direction VARCHAR). Downstream callers
// rely on the field names (`value`, `unit`, `direction`) and on `direction`
// taking string values from a small fixed vocabulary. The unit field uses
// the canonical singular form of each unit name.
//
// Initial implementation: same coverage as FormatRelative — only the three
// smallest units, only the past direction (so `direction` is always "past"),
// and no special-case for negligible deltas. Larger units, future direction,
// the negligible-delta sentinel, and additional input-type overloads are
// not yet implemented.
struct PartsResult {
	int64_t value;
	const char *unit;
	const char *direction;
};

PartsResult ComputePartsRelative(timestamp_t target, timestamp_t reference) {
	int64_t target_us = Timestamp::GetEpochMicroSeconds(target);
	int64_t ref_us = Timestamp::GetEpochMicroSeconds(reference);
	int64_t delta_us = ref_us - target_us;
	int64_t delta_sec = delta_us / Interval::MICROS_PER_SEC;

	if (delta_sec <= Interval::SECS_PER_MINUTE) {
		return {delta_sec, "second", "past"};
	}

	int64_t delta_min = delta_sec / Interval::SECS_PER_MINUTE;
	if (delta_min <= Interval::MINS_PER_HOUR) {
		return {delta_min, "minute", "past"};
	}

	int64_t delta_hr = delta_min / Interval::MINS_PER_HOUR;
	return {delta_hr, "hour", "past"};
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
		    StringVector::AddString(unit_vec, parts.unit);
		FlatVector::GetDataMutable<string_t>(dir_vec)[i] =
		    StringVector::AddString(dir_vec, parts.direction);
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
