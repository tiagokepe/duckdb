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
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {

    struct IndexJoinInner {
        static index_t Perform(DataChunk &result_chunk,index_t &lpos, DataChunk &left_conditions,
                                               vector<JoinCondition> &conditions,TableCatalogEntry &tableref, DataTable &table, Index &index, vector<column_t> column_ids );

    };


} // namespace duckdb
