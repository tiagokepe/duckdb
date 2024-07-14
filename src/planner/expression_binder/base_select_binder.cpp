#include "duckdb/planner/expression_binder/base_select_binder.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression_binder/aggregate_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/expression_binder/select_bind_state.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
namespace duckdb {

BaseSelectBinder::BaseSelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node,
                                   BoundGroupInformation &info)
    : ExpressionBinder(binder, context), inside_window(false), node(node), info(info) {
}

LogicalType ExpressionBinder::GetResultCollation(const BoundFunctionExpression &function) {
	LogicalType result_type = function.return_type;
	string last_collation;
	for (auto &child: function.children) {
		auto child_ret_type = child->return_type;
		if (StringType::IsCollated(child_ret_type)) {
			auto collation = StringType::GetCollation(child_ret_type);
			if (!last_collation.empty() && last_collation != collation) {
				throw BinderException(function, "Function \"%s\" has multiple collations: %s and %s",
									function.function.name, last_collation, collation);
			}
			last_collation = collation;
			result_type = child_ret_type;
		}
	}
	return result_type;
}

BindResult BaseSelectBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	// check if the expression binds to one of the groups
	auto group_index = TryBindGroup(expr);
	if (group_index != DConstants::INVALID_INDEX) {
		return BindGroup(expr, depth, group_index);
	}
	switch (expr.expression_class) {
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef(expr_ptr, depth, root_expression);
	case ExpressionClass::DEFAULT:
		return BindResult("SELECT clause cannot contain DEFAULT clause");
	case ExpressionClass::FUNCTION:
		return BindFunction(expr_ptr, depth, root_expression);
	case ExpressionClass::WINDOW:
		return BindWindow(expr.Cast<WindowExpression>(), depth);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth, root_expression);
	}
}

idx_t BaseSelectBinder::TryBindGroup(ParsedExpression &expr) {
	// first check the group alias map, if expr is a ColumnRefExpression
	if (expr.type == ExpressionType::COLUMN_REF) {
		auto &colref = expr.Cast<ColumnRefExpression>();
		if (!colref.IsQualified()) {
			auto alias_entry = info.alias_map.find(colref.column_names[0]);
			if (alias_entry != info.alias_map.end()) {
				// found entry!
				return alias_entry->second;
			}
		}
	}
	// no alias reference found
	// check the list of group columns for a match
	auto entry = info.map.find(expr);
	if (entry != info.map.end()) {
		return entry->second;
	}
#ifdef DEBUG
	for (auto map_entry : info.map) {
		D_ASSERT(!map_entry.first.get().Equals(expr));
		D_ASSERT(!expr.Equals(map_entry.first.get()));
	}
#endif
	return DConstants::INVALID_INDEX;
}

BindResult BaseSelectBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	return ExpressionBinder::BindExpression(expr_ptr, depth);
}

bool BaseSelectBinder::IsExtraOrderbyEntry(ParsedExpression &expr) {
	// true if expr is an extra entry added to the select list from the OrderBinder
	return (node.bind_state.orderby_select_entry.find(expr) != node.bind_state.orderby_select_entry.end());
}

bool BaseSelectBinder::CanPushCollation(ParsedExpression &expr, LogicalType return_type) {
	switch (return_type.id()) {
	case LogicalTypeId::BOOLEAN:
		// we can push collation when the function result type is BOOLEAN
		return true;
	default:
		return IsExtraOrderbyEntry(expr);
	}
}

BindResult BaseSelectBinder::BindFunction(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	D_ASSERT(expr_ptr->expression_class == ExpressionClass::FUNCTION);
	auto bound_result = ExpressionBinder::BindExpression(expr_ptr, depth, root_expression);
	auto &bound_function = bound_result.expression->Cast<BoundFunctionExpression>();

	auto result_type = GetResultCollation(bound_function);
	bool can_push_collation = CanPushCollation(*expr_ptr, bound_function.return_type);
	if (can_push_collation) {
		for (auto &child: bound_function.children) {
			if (child->return_type.id() == LogicalTypeId::VARCHAR) {
				ExpressionBinder::PushCollation(context, child, result_type, false);
				// child->return_type = result_type;
			}
		}
	}
	// propagating result child collation to function result type
	if (bound_function.return_type.id() == LogicalTypeId::VARCHAR) {
		bound_function.return_type = result_type;
	}
	return bound_result;
}

BindResult BaseSelectBinder::BindGroupingFunction(OperatorExpression &op, idx_t depth) {
	if (op.children.empty()) {
		throw InternalException("GROUPING requires at least one child");
	}
	if (node.groups.group_expressions.empty()) {
		return BindResult(BinderException(op, "GROUPING statement cannot be used without groups"));
	}
	if (op.children.size() >= 64) {
		return BindResult(BinderException(op, "GROUPING statement cannot have more than 64 groups"));
	}
	vector<idx_t> group_indexes;
	group_indexes.reserve(op.children.size());
	for (auto &child : op.children) {
		ExpressionBinder::QualifyColumnNames(binder, child);
		auto idx = TryBindGroup(*child);
		if (idx == DConstants::INVALID_INDEX) {
			return BindResult(BinderException(op, "GROUPING child \"%s\" must be a grouping column", child->GetName()));
		}
		group_indexes.push_back(idx);
	}
	auto col_idx = node.grouping_functions.size();
	node.grouping_functions.push_back(std::move(group_indexes));
	return BindResult(make_uniq<BoundColumnRefExpression>(op.GetName(), LogicalType::BIGINT,
	                                                      ColumnBinding(node.groupings_index, col_idx), depth));
}

BindResult BaseSelectBinder::BindGroup(ParsedExpression &expr, idx_t depth, idx_t group_index) {
	auto it = info.collated_groups.find(group_index);
	if (it != info.collated_groups.end()) {
		// This is an implicitly collated group, so we need to refer to the first() aggregate
		const auto &aggr_index = it->second;
		auto uncollated_first_expression =
		    make_uniq<BoundColumnRefExpression>(expr.GetName(), node.aggregates[aggr_index]->return_type,
		                                        ColumnBinding(node.aggregate_index, aggr_index), depth);

		auto &group_expr = node.groups.group_expressions[group_index];

		if (uncollated_first_expression->return_type.id() == LogicalTypeId::VARCHAR &&
			group_expr->expression_class == ExpressionClass::BOUND_FUNCTION) {

			auto &func_expr = group_expr->Cast<BoundFunctionExpression>();
			LogicalType result_type = ExpressionBinder::GetResultCollation(func_expr);
			uncollated_first_expression->return_type = result_type;
		}

		if (node.groups.grouping_sets.size() <= 1) {
			// if there are no more than two grouping sets, you can return the uncollated first expression.
			// "first" meaning the aggreagte function.
			return BindResult(std::move(uncollated_first_expression));
		}

		// otherwise we insert a case statement to return NULL when the collated group expression is NULL
		// otherwise you can return the "first" of the uncollated expression.
		auto &group = node.groups.group_expressions[group_index];
		auto collated_group_expression = make_uniq<BoundColumnRefExpression>(
		    expr.GetName(), group->return_type, ColumnBinding(node.group_index, group_index), depth);

		auto sql_null = make_uniq<BoundConstantExpression>(Value(LogicalType::VARCHAR));
		auto when_expr = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
		when_expr->children.push_back(std::move(collated_group_expression));
		auto then_expr = make_uniq<BoundConstantExpression>(Value(LogicalType::VARCHAR));
		auto else_expr = std::move(uncollated_first_expression);
		auto case_expr =
		    make_uniq<BoundCaseExpression>(std::move(when_expr), std::move(then_expr), std::move(else_expr));
		return BindResult(std::move(case_expr));
	} else {
		auto &group = node.groups.group_expressions[group_index];
		return BindResult(make_uniq<BoundColumnRefExpression>(expr.GetName(), group->return_type,
		                                                      ColumnBinding(node.group_index, group_index), depth));
	}
}

} // namespace duckdb
