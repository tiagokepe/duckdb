//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

    struct IndexJoinInner {
        static index_t Perform(index_t &ltuple, index_t &rtuple, DataChunk &left_conditions, DataChunk &right_conditions,
                               sel_t lvector[], sel_t rvector[], vector<JoinCondition> &conditions);
    };


} // namespace duckdb
