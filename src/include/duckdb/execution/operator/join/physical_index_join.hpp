//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_index_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {

//    index_t nested_loop_join(ExpressionType op, Vector &left, Vector &right, index_t &lpos, index_t &rpos, sel_t lvector[],
//                             sel_t rvector[]);
//    index_t nested_loop_comparison(ExpressionType op, Vector &left, Vector &right, sel_t lvector[], sel_t rvector[],
//                                   index_t count);

//! PhysicalIndexJoin represents an index nested loop join between two tables
    class PhysicalIndexJoin : public PhysicalComparisonJoin {
    public:
    //! In case we do an index nested loop join we need to hold the following information:
    //! The table to scan
    TableCatalogEntry &tableref;
    //! The physical data table to scan
    DataTable &table;
    //! The index to use for the scan
    Index &index;
    //! The column ids to project
    vector<column_t> column_ids;

    PhysicalIndexJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                           vector<JoinCondition> cond, JoinType join_type,TableCatalogEntry &tableref, DataTable &table, Index &index,
                      vector<column_t> column_ids): PhysicalComparisonJoin(op, PhysicalOperatorType::INDEX_JOIN, move(cond), join_type),tableref(tableref), table(table), index(index),
                column_ids(column_ids){
        children.push_back(move(left));
    };

    public:
        void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
        unique_ptr<PhysicalOperatorState> GetOperatorState() override;
    };

} // namespace duckdb
