#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::PlanFilter(unique_ptr<Expression> condition, unique_ptr<LogicalOperator> root) {
	PlanSubqueries(condition, root);
	auto filter = make_uniq<LogicalFilter>(std::move(condition));
	filter->AddChild(std::move(root));
	return std::move(filter);
}

void Binder::PlanCollatedSubquery(BoundSelectNode &statement, unique_ptr<Expression> &group, idx_t group_idx) {
	auto entry = statement.groups.collated_subquery_group.find(group_idx);
	if (entry != statement.groups.collated_subquery_group.end()) {
		D_ASSERT(group->expression_class == ExpressionClass::BOUND_FUNCTION);
		auto &func_expr = group->Cast<BoundFunctionExpression>();

		D_ASSERT(!func_expr.children.empty());
		auto &child = func_expr.children[0];

		auto first_fun = FirstFun::GetFunction(LogicalType::VARCHAR);
		vector<unique_ptr<Expression>> first_children;
		first_children.push_back(child->Copy());

		FunctionBinder function_binder(context);
		auto function = function_binder.BindAggregateFunction(first_fun, std::move(first_children));
		statement.aggregates.push_back(std::move(function));
	}
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundSelectNode &statement) {
	unique_ptr<LogicalOperator> root;
	D_ASSERT(statement.from_table);
	root = CreatePlan(*statement.from_table);
	D_ASSERT(root);

	// plan the sample clause
	if (statement.sample_options) {
		root = make_uniq<LogicalSample>(std::move(statement.sample_options), std::move(root));
	}

	if (statement.where_clause) {
		root = PlanFilter(std::move(statement.where_clause), std::move(root));
	}

	if (!statement.aggregates.empty() || !statement.groups.group_expressions.empty()) {
		if (!statement.groups.group_expressions.empty()) {
			// visit the groups
			for (idx_t group_idx = 0; group_idx < statement.groups.group_expressions.size(); group_idx++) {
				auto &group = statement.groups.group_expressions[group_idx];
				PlanSubqueries(group, root);
				PlanCollatedSubquery(statement, group, group_idx);
			}
		}
		// now visit all aggregate expressions
		for (auto &expr : statement.aggregates) {
			PlanSubqueries(expr, root);
		}
		// finally create the aggregate node with the group_index and aggregate_index as obtained from the binder
		auto aggregate = make_uniq<LogicalAggregate>(statement.group_index, statement.aggregate_index,
		                                             std::move(statement.aggregates));
		aggregate->groups = std::move(statement.groups.group_expressions);
		aggregate->groupings_index = statement.groupings_index;
		aggregate->grouping_sets = std::move(statement.groups.grouping_sets);
		aggregate->grouping_functions = std::move(statement.grouping_functions);

		aggregate->AddChild(std::move(root));
		root = std::move(aggregate);
	} else if (!statement.groups.grouping_sets.empty()) {
		// edge case: we have grouping sets but no groups or aggregates
		// this can only happen if we have e.g. select 1 from tbl group by ();
		// just output a dummy scan
		root = make_uniq_base<LogicalOperator, LogicalDummyScan>(statement.group_index);
	}

	if (statement.having) {
		PlanSubqueries(statement.having, root);
		auto having = make_uniq<LogicalFilter>(std::move(statement.having));

		having->AddChild(std::move(root));
		root = std::move(having);
	}

	if (!statement.windows.empty()) {
		auto win = make_uniq<LogicalWindow>(statement.window_index);
		win->expressions = std::move(statement.windows);
		// visit the window expressions
		for (auto &expr : win->expressions) {
			PlanSubqueries(expr, root);
		}
		D_ASSERT(!win->expressions.empty());
		win->AddChild(std::move(root));
		root = std::move(win);
	}

	if (statement.qualify) {
		PlanSubqueries(statement.qualify, root);
		auto qualify = make_uniq<LogicalFilter>(std::move(statement.qualify));

		qualify->AddChild(std::move(root));
		root = std::move(qualify);
	}

	for (idx_t i = statement.unnests.size(); i > 0; i--) {
		auto unnest_level = i - 1;
		auto entry = statement.unnests.find(unnest_level);
		if (entry == statement.unnests.end()) {
			throw InternalException("unnests specified at level %d but none were found", unnest_level);
		}
		auto &unnest_node = entry->second;
		auto unnest = make_uniq<LogicalUnnest>(unnest_node.index);
		unnest->expressions = std::move(unnest_node.expressions);
		// visit the unnest expressions
		for (auto &expr : unnest->expressions) {
			PlanSubqueries(expr, root);
		}
		D_ASSERT(!unnest->expressions.empty());
		unnest->AddChild(std::move(root));
		root = std::move(unnest);
	}

	for (auto &expr : statement.select_list) {
		PlanSubqueries(expr, root);
	}

	auto proj = make_uniq<LogicalProjection>(statement.projection_index, std::move(statement.select_list));
	auto &projection = *proj;
	proj->AddChild(std::move(root));
	root = std::move(proj);

	// finish the plan by handling the elements of the QueryNode
	root = VisitQueryNode(statement, std::move(root));

	// add a prune node if necessary
	if (statement.need_prune) {
		D_ASSERT(root);
		vector<unique_ptr<Expression>> prune_expressions;
		for (idx_t i = 0; i < statement.column_count; i++) {
			prune_expressions.push_back(make_uniq<BoundColumnRefExpression>(
			    projection.expressions[i]->return_type, ColumnBinding(statement.projection_index, i)));
		}
		auto prune = make_uniq<LogicalProjection>(statement.prune_index, std::move(prune_expressions));
		prune->AddChild(std::move(root));
		root = std::move(prune);
	}
	return root;
}

} // namespace duckdb
