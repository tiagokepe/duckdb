//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/core_functions/aggregate/nested_functions.hpp
//
//
//===----------------------------------------------------------------------===//
// This file is automatically generated by scripts/generate_functions.py
// Do not edit this file manually, your changes will be overwritten
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct HistogramFun {
	static constexpr const char *Name = "histogram";
	static constexpr const char *Parameters = "arg";
	static constexpr const char *Description = "Returns a LIST of STRUCTs with the fields bucket and count.";
	static constexpr const char *Example = "histogram(A)";

	static AggregateFunctionSet GetFunctions();
	static AggregateFunction GetHistogramUnorderedMap(LogicalType &type);
	static AggregateFunction BinnedHistogramFunction();
};

struct ListFun {
	static constexpr const char *Name = "list";
	static constexpr const char *Parameters = "arg";
	static constexpr const char *Description = "Returns a LIST containing all the values of a column.";
	static constexpr const char *Example = "list(A)";

	static AggregateFunction GetFunction();
};

struct ArrayAggFun {
	using ALIAS = ListFun;

	static constexpr const char *Name = "array_agg";
};

} // namespace duckdb
