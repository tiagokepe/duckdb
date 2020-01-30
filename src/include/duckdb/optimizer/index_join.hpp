//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/index_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/optimizer/rule.hpp"

namespace duckdb {
class Optimizer;

class IndexJoin {
public:
	//! Optimize Equality Join + Filter in IndexJoin
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> node);

private:
	//! Transform a Equality Join + Filter in IndexJoin
	unique_ptr<LogicalOperator> TransformJoinToIndexJoin(unique_ptr<LogicalOperator> op);
};

} // namespace duckdb
