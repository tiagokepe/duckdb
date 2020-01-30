//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_index_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//! LogicalIndexJoin represents a join between two relations when RHS has an index on the join key
class LogicalIndexJoin : public LogicalJoin {
public:
	LogicalIndexJoin(JoinType type, vector<index_t> left_projection_map, vector<index_t> right_projection_map,
	                 TableCatalogEntry &tableref, DataTable &table, Index &index, vector<column_t> column_ids,
	                 index_t table_index)
	    : LogicalJoin(type, LogicalOperatorType::INDEX_JOIN), left_projection_map(left_projection_map),
	      right_projection_map(right_projection_map), tableref(tableref), table(table), index(index),
	      column_ids(column_ids), table_index(table_index){};

	//! The conditions of the join
	vector<JoinCondition> conditions;
	//! The table to scan
	TableCatalogEntry &tableref;
	//! The physical data table to scan
	DataTable &table;
	//! The index to use for the scan
	Index &index;
	//! The column ids to project
	vector<column_t> column_ids;
	//! The value for the query predicate
	Value low_value;
	Value high_value;
	Value equal_value;

	//! If the predicate is low, high or equal
	bool low_index = false;
	bool high_index = false;
	bool equal_index = false;

	//! The expression type (e.g., >, <, >=, <=)
	ExpressionType low_expression_type;
	ExpressionType high_expression_type;

	//! The table index in the current bind context
	index_t table_index;

protected:
	void ResolveTypes() override {
		if (column_ids.size() == 0) {
			types = {TypeId::INTEGER};
		} else {
			types = tableref.GetTypes(column_ids);
		}
	}
};

} // namespace duckdb
