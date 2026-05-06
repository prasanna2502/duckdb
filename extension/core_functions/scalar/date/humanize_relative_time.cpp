#include "core_functions/scalar/date_functions.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
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

} // namespace

ScalarFunction HumanizeRelativeTimeFun::GetFunction() {
	return ScalarFunction("humanize_relative_time",
	                      {LogicalType::TIMESTAMP, LogicalType::TIMESTAMP},
	                      LogicalType::VARCHAR, HumanizeRelativeTimeFunction);
}

} // namespace duckdb
