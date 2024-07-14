#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/expression_binder/column_alias_binder.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

namespace duckdb {

WhereBinder::WhereBinder(Binder &binder, ClientContext &context, optional_ptr<ColumnAliasBinder> column_alias_binder)
    : ExpressionBinder(binder, context), column_alias_binder(column_alias_binder) {
	target_type = LogicalType(LogicalTypeId::BOOLEAN);
}

BindResult WhereBinder::BindColumnRef(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {

	auto result = ExpressionBinder::BindExpression(expr_ptr, depth);
	if (!result.HasError() || !column_alias_binder) {
		return result;
	}

	BindResult alias_result;
	auto found_alias = column_alias_binder->BindAlias(*this, expr_ptr, depth, root_expression, alias_result);
	if (found_alias) {
		return alias_result;
	}

	return result;
}

BindResult WhereBinder::BindFunction(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	D_ASSERT(expr_ptr->expression_class == ExpressionClass::FUNCTION);
	auto bound_result = ExpressionBinder::BindExpression(expr_ptr, depth, root_expression);

	auto &bound_expr = bound_result.expression;
	auto &bound_function = bound_expr->Cast<BoundFunctionExpression>();

	auto result_type = GetResultCollation(bound_function);
	for (auto &child: bound_function.children) {
		if (child->return_type.id() == LogicalTypeId::VARCHAR) {
			ExpressionBinder::PushCollation(context, child, result_type, false);
			child->return_type = result_type;
		}
	}
	if (bound_function.return_type.id() == LogicalTypeId::VARCHAR && StringType::IsCollated(result_type)) {
			if(StringType::IsCollated(bound_function.return_type) && bound_function.return_type != result_type) {
					throw BinderException(bound_function, "Function \"%s\" has multiple collations: %s and %s",
											bound_function.function.name, StringType::GetCollation(bound_function.return_type),
																StringType::GetCollation(result_type));
            }
		// propagating result child collation to function result type
		bound_function.return_type = result_type;
		ExpressionBinder::PushCollation(context, bound_expr, result_type, false);
	}

	return bound_result;
}

BindResult WhereBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth, bool root_expression) {
	auto &expr = *expr_ptr;
	switch (expr.GetExpressionClass()) {
	case ExpressionClass::DEFAULT:
		return BindResult("WHERE clause cannot contain DEFAULT clause");
	case ExpressionClass::WINDOW:
		return BindResult("WHERE clause cannot contain window functions!");
	case ExpressionClass::COLUMN_REF:
		return BindColumnRef(expr_ptr, depth, root_expression);
	case ExpressionClass::FUNCTION:
		return BindFunction(expr_ptr, depth, root_expression);
	default:
		return ExpressionBinder::BindExpression(expr_ptr, depth);
	}
}

string WhereBinder::UnsupportedAggregateMessage() {
	return "WHERE clause cannot contain aggregates!";
}

} // namespace duckdb
