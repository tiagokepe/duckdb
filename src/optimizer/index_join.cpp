#include "duckdb/optimizer/index_join.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"

#include "duckdb/parser/expression/comparison_expression.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_index_join.hpp"

#include "duckdb/storage/data_table.hpp"
using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> IndexJoin::Optimize(unique_ptr<LogicalOperator> op) {
	// We need to check if the current node is a Join and if either the left side or the right side is a get
	if (op->type == LogicalOperatorType::COMPARISON_JOIN &&
	    (op->children[0]->type == LogicalOperatorType::GET || op->children[1]->type == LogicalOperatorType::GET)) {
		return TransformJoinToIndexJoin(move(op));
	}
	for (auto &child : op->children) {
		child = Optimize(move(child));
	}
	return op;
}

// FIXME: This rewrite rule shouldn't exist, during execution time we should automatically go from an index-join to a
// hash-join
// Hence, here we only check a simple case to ensure that the index-join is used in our unit-tests and benchmarks.
unique_ptr<LogicalOperator> IndexJoin::TransformJoinToIndexJoin(unique_ptr<LogicalOperator> op) {
	assert(op->type == LogicalOperatorType::COMPARISON_JOIN);
	auto &join = (LogicalComparisonJoin &)*op;
	auto get = (LogicalGet *)op->children[0].get();

	if (!get->table) {
		return op;
	}

	auto &storage = *get->table->storage;

	if (storage.indexes.size() == 0) {
		// no indexes on the table, can't rewrite
		return op;
	}
	// FIXME right now I only check if there is only one join-key condition
	if (join.conditions.size() > 1) {
		return op;
	}
	// check all the indexes
	for (size_t j = 0; j < storage.indexes.size(); j++) {
		auto &index = storage.indexes[j];

		// first rewrite the index expression so the ColumnBindings align with the column bindings of the current table
		if (index->unbound_expressions.size() > 1)
			continue;
		auto index_expression = index->unbound_expressions[0]->Copy();
		bool rewrite_possible = true;
		RewriteIndexExpression(*index, *get, *index_expression, rewrite_possible);
		if (!rewrite_possible) {
			// could not rewrite!
			continue;
		}

		Value low_value, high_value, equal_value;
		// try to find a matching index for any of the filter expressions

		// FIXME: Right now I only check the LHS for the join key
        //FIXME: Might need to swap conditions in the optimizer

        auto expr = (BoundColumnRefExpression *)join.conditions[0].left.get();
		auto idx_bindings = (BoundColumnRefExpression *)index_expression.get();
		// Its a match
		if (expr->binding.table_index == idx_bindings->binding.table_index &&
		    expr->binding.column_index == idx_bindings->binding.column_index) {
			auto logical_index_join = make_unique<LogicalIndexJoin>(
			    join.join_type, join.left_projection_map, join.right_projection_map,join.conditions, *get->table, *get->table->storage,
			    *index, get->column_ids, get->table_index);
			logical_index_join->children.push_back(move(join.children[1]));
			op = move(logical_index_join);
		}
		return op;
	}
	return op;
}
