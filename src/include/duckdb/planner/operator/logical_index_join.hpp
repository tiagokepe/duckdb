//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_index_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {

//! LogicalIndexJoin represents a join between two relations when RHS has an index on the join key
class LogicalIndexJoin : public LogicalJoin {
public:
    //! The conditions of the join
    vector<JoinCondition> conditions;
	LogicalIndexJoin(JoinType type, vector<index_t> left_projection_map, vector<index_t> right_projection_map,
                     vector<JoinCondition> &conditions,
	                 TableCatalogEntry &tableref, DataTable &table, Index &index, vector<column_t> column_ids,
	                 index_t table_index)
	    : LogicalJoin(type, LogicalOperatorType::INDEX_JOIN), conditions(move(conditions)), tableref(tableref), table(table), index(index),
	      column_ids(std::move(column_ids)), table_index(table_index){
	    this->left_projection_map = left_projection_map;
        this->right_projection_map = right_projection_map;
	};


	//! The table to scan
	TableCatalogEntry &tableref;
	//! The physical data table to scan
	DataTable &table;
	//! The index to use for the scan
	Index &index;
	//! The column ids to project
	vector<column_t> column_ids;
	//! The table index in the current bind context
	index_t table_index;

    vector<ColumnBinding> GetColumnBindings() override {
        // Get bindings from LHS
        auto left_bindings = MapBindings(children[0]->GetColumnBindings(), left_projection_map);
        // Get bindings from Index
        auto idx_bindings =  MapBindings(GenerateColumnBindings(table_index, column_ids.size()), right_projection_map);
        left_bindings.insert(left_bindings.end(), idx_bindings.begin(), idx_bindings.end());
        return left_bindings;
    }

protected:
    void ResolveTypes() override {
        types = MapTypes(children[0]->types, left_projection_map);
    }
};

} // namespace duckdb
