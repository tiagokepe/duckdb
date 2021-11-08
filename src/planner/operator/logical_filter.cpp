#include "duckdb/planner/operator/logical_filter.hpp"

#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

LogicalFilter::LogicalFilter(unique_ptr<Expression> expression) : LogicalOperator(LogicalOperatorType::LOGICAL_FILTER) {
	expressions.push_back(move(expression));
	SplitPredicates(expressions);
}

LogicalFilter::LogicalFilter() : LogicalOperator(LogicalOperatorType::LOGICAL_FILTER) {
}

void LogicalFilter::ResolveTypes() {
	types = MapTypes(children[0]->types, projection_map);
}

vector<ColumnBinding> LogicalFilter::GetColumnBindings() {
	return MapBindings(children[0]->GetColumnBindings(), projection_map);
}

void LogicalFilter::OrPredicate(BoundConjunctionExpression *conjunction) {
	// TODO adicionar colunas no MAP independente de onde estiver
	// esquerda ou direira, aninhada ou não... parece que não faz diferença se fizermos:
	// aiddionar AND (A=1 OR A=2) AND (B>2 OR B<2) AND (C!=3 OR C=3) ...
	for (idx_t k = 0; k < conjunction->children.size(); k++) {
		auto ch_exp = conjunction->children[k].get();
		auto name = ch_exp->GetName();
		auto str = ch_exp->ToString();
		if (conjunction->children[k]->GetExpressionClass() == ExpressionClass::BOUND_CONJUNCTION) {
			OrPredicate((BoundConjunctionExpression *)conjunction->children[k].get());
		}

		if (conjunction->children[k]->type == ExpressionType::BOUND_COLUMN_REF) {
			auto class_expr = conjunction->children[k]->GetExpressionClass();
			auto col_ref = (BoundColumnRefExpression *)conjunction->children[k].get();
		}
		if (conjunction->children[k]->GetExpressionClass() == ExpressionClass::BOUND_COMPARISON) {
			auto comparison = (BoundComparisonExpression *)conjunction->children[k].get();
			if (comparison->type != ExpressionType::COMPARE_LESSTHAN &&
			    comparison->type != ExpressionType::COMPARE_LESSTHANOREQUALTO &&
			    comparison->type != ExpressionType::COMPARE_GREATERTHAN &&
			    comparison->type != ExpressionType::COMPARE_GREATERTHANOREQUALTO &&
			    comparison->type != ExpressionType::COMPARE_EQUAL &&
			    comparison->type != ExpressionType::COMPARE_NOTEQUAL) {
				// only support [>, >=, <, <=, ==] expressions
				return;
			}

			// check if one of the sides is a scalar value
			bool left_is_scalar = comparison->left->IsFoldable();
			bool right_is_scalar = comparison->right->IsFoldable();
			if (left_is_scalar || right_is_scalar) {
				// comparison with scalar
				auto non_scalar = left_is_scalar ? comparison->right.get() : comparison->left.get();
				auto scalar = left_is_scalar ? comparison->left.get() : comparison->right.get();
				auto constant_value = ExpressionExecutor::EvaluateScalar(*scalar);

				// create the ExpressionValueInformation
				// ExpressionValueInformation info;
				// info.comparison_type = left_is_scalar ? FlipComparisionExpression(comparison.type) : comparison.type;
				// info.constant = constant_value;
			}
		}
	}
}

// Split the predicates separated by AND statements
// These are the predicates that are safe to push down because all of them MUST
// be true
bool LogicalFilter::SplitPredicates(vector<unique_ptr<Expression>> &expressions) {
	bool found_conjunction = false;
	for (idx_t i = 0; i < expressions.size(); i++) {
		switch (expressions[i]->type) {
		case ExpressionType::CONJUNCTION_AND: {
			auto &conjunction = (BoundConjunctionExpression &)*expressions[i];
			found_conjunction = true;
			// AND expression, append the other children
			for (idx_t k = 1; k < conjunction.children.size(); k++) {
				expressions.push_back(move(conjunction.children[k]));
			}
			// replace this expression with the first child of the conjunction
			expressions[i] = move(conjunction.children[0]);
			// we move back by one so the right child is checked again
			// in case it is an AND expression as well
			i--;
			break;
		}
		// case ExpressionType::CONJUNCTION_OR: {
		// 	auto &conjunction = (BoundConjunctionExpression &)*expressions[i];
		// 	OrPredicate(conjunction);
		// 	break;
		// }
		default: {
			//break;
		}
		}
	}
	return found_conjunction;
}

} // namespace duckdb
