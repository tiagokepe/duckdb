#include "duckdb/parser/expression/collate_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_binder/order_binder.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"
#include "duckdb/planner/query_node/bound_set_operation_node.hpp"
#include "duckdb/planner/expression_binder/select_bind_state.hpp"
#include "duckdb/common/enum_util.hpp"


namespace duckdb {

static void GatherAliases(BoundQueryNode &node, SelectBindState &bind_state, const vector<idx_t> &reorder_idx) {
	if (node.type == QueryNodeType::SET_OPERATION_NODE) {
		// setop, recurse
		auto &setop = node.Cast<BoundSetOperationNode>();

		// create new reorder index
		if (setop.setop_type == SetOperationType::UNION_BY_NAME) {
			vector<idx_t> new_left_reorder_idx(setop.left_reorder_idx.size());
			vector<idx_t> new_right_reorder_idx(setop.right_reorder_idx.size());
			for (idx_t i = 0; i < setop.left_reorder_idx.size(); ++i) {
				new_left_reorder_idx[i] = reorder_idx[setop.left_reorder_idx[i]];
			}

			for (idx_t i = 0; i < setop.right_reorder_idx.size(); ++i) {
				new_right_reorder_idx[i] = reorder_idx[setop.right_reorder_idx[i]];
			}

			// use new reorder index
			GatherAliases(*setop.left, bind_state, new_left_reorder_idx);
			GatherAliases(*setop.right, bind_state, new_right_reorder_idx);
			return;
		}

		GatherAliases(*setop.left, bind_state, reorder_idx);
		GatherAliases(*setop.right, bind_state, reorder_idx);
	} else {
		// query node
		D_ASSERT(node.type == QueryNodeType::SELECT_NODE);
		auto &select = node.Cast<BoundSelectNode>();
		// fill the alias lists with the names
		for (idx_t i = 0; i < select.names.size(); i++) {
			auto &name = select.names[i];
			// first check if the alias is already in there
			auto entry = bind_state.alias_map.find(name);

			idx_t index = reorder_idx[i];

			if (entry == bind_state.alias_map.end()) {
				// the alias is not in there yet, just assign it
				bind_state.alias_map[name] = index;
			}
		}
		// check if the expression matches one of the expressions in the original expression liset
		for (idx_t i = 0; i < select.bind_state.original_expressions.size(); i++) {
			auto &expr = select.bind_state.original_expressions[i];
			idx_t index = reorder_idx[i];
			// now check if the node is already in the set of expressions
			auto expr_entry = bind_state.projection_map.find(*expr);
			if (expr_entry != bind_state.projection_map.end()) {
				// the node is in there
				// repeat the same as with the alias: if there is an ambiguity we insert "-1"
				if (expr_entry->second != index) {
					bind_state.projection_map[*expr] = DConstants::INVALID_INDEX;
				}
			} else {
				// not in there yet, just place it in there
				bind_state.projection_map[*expr] = index;
			}
		}
	}
}

static void BuildUnionByNameInfo(ClientContext &context, BoundSetOperationNode &result, bool can_contain_nulls) {
	D_ASSERT(result.setop_type == SetOperationType::UNION_BY_NAME);
	case_insensitive_map_t<idx_t> left_names_map;
	case_insensitive_map_t<idx_t> right_names_map;

	auto &left_node = *result.left;
	auto &right_node = *result.right;

	// Build a name_map to use to check if a name exists
	// We throw a binder exception if two same name in the SELECT list
	for (idx_t i = 0; i < left_node.names.size(); ++i) {
		if (left_names_map.find(left_node.names[i]) != left_names_map.end()) {
			throw BinderException("UNION(ALL) BY NAME operation doesn't support same name in SELECT list");
		}
		left_names_map[left_node.names[i]] = i;
	}

	for (idx_t i = 0; i < right_node.names.size(); ++i) {
		if (right_names_map.find(right_node.names[i]) != right_names_map.end()) {
			throw BinderException("UNION(ALL) BY NAME operation doesn't support same name in SELECT list");
		}
		if (left_names_map.find(right_node.names[i]) == left_names_map.end()) {
			result.names.push_back(right_node.names[i]);
		}
		right_names_map[right_node.names[i]] = i;
	}

	idx_t new_size = result.names.size();
	bool need_reorder = false;
	vector<idx_t> left_reorder_idx(left_node.names.size());
	vector<idx_t> right_reorder_idx(right_node.names.size());

	// Construct return type and reorder_idxs
	// reorder_idxs is used to gather correct alias_map
	// and expression_map in GatherAlias(...)
	for (idx_t i = 0; i < new_size; ++i) {
		auto left_index = left_names_map.find(result.names[i]);
		auto right_index = right_names_map.find(result.names[i]);
		bool left_exist = left_index != left_names_map.end();
		bool right_exist = right_index != right_names_map.end();
		LogicalType result_type;
		if (left_exist && right_exist) {
			result_type = LogicalType::ForceMaxLogicalType(left_node.types[left_index->second],
			                                               right_node.types[right_index->second]);
			if (left_index->second != i || right_index->second != i) {
				need_reorder = true;
			}
			left_reorder_idx[left_index->second] = i;
			right_reorder_idx[right_index->second] = i;
		} else if (left_exist) {
			result_type = left_node.types[left_index->second];
			need_reorder = true;
			left_reorder_idx[left_index->second] = i;
		} else {
			D_ASSERT(right_exist);
			result_type = right_node.types[right_index->second];
			need_reorder = true;
			right_reorder_idx[right_index->second] = i;
		}

		if (!can_contain_nulls) {
			if (ExpressionBinder::ContainsNullType(result_type)) {
				result_type = ExpressionBinder::ExchangeNullType(result_type);
			}
		}

		result.types.push_back(result_type);
	}

	result.left_reorder_idx = std::move(left_reorder_idx);
	result.right_reorder_idx = std::move(right_reorder_idx);

	// If reorder is required, collect reorder expressions for push projection
	// into the two child nodes of union node
	if (need_reorder) {
		for (idx_t i = 0; i < new_size; ++i) {
			auto left_index = left_names_map.find(result.names[i]);
			auto right_index = right_names_map.find(result.names[i]);
			bool left_exist = left_index != left_names_map.end();
			bool right_exist = right_index != right_names_map.end();
			unique_ptr<Expression> left_reorder_expr;
			unique_ptr<Expression> right_reorder_expr;
			if (left_exist && right_exist) {
				left_reorder_expr = make_uniq<BoundColumnRefExpression>(
				    left_node.types[left_index->second], ColumnBinding(left_node.GetRootIndex(), left_index->second));
				right_reorder_expr =
				    make_uniq<BoundColumnRefExpression>(right_node.types[right_index->second],
				                                        ColumnBinding(right_node.GetRootIndex(), right_index->second));
			} else if (left_exist) {
				left_reorder_expr = make_uniq<BoundColumnRefExpression>(
				    left_node.types[left_index->second], ColumnBinding(left_node.GetRootIndex(), left_index->second));
				// create null value here
				right_reorder_expr = make_uniq<BoundConstantExpression>(Value(result.types[i]));
			} else {
				D_ASSERT(right_exist);
				left_reorder_expr = make_uniq<BoundConstantExpression>(Value(result.types[i]));
				right_reorder_expr =
				    make_uniq<BoundColumnRefExpression>(right_node.types[right_index->second],
				                                        ColumnBinding(right_node.GetRootIndex(), right_index->second));
			}
			result.left_reorder_exprs.push_back(std::move(left_reorder_expr));
			result.right_reorder_exprs.push_back(std::move(right_reorder_expr));
		}
	}
}

void Binder::BindCollationGroup(unique_ptr<BoundSetOperationNode> &bound_set_op) {
	if (bound_set_op->left->type != QueryNodeType::SELECT_NODE ||
		bound_set_op->right->type != QueryNodeType::SELECT_NODE) {
		return;
	}
	auto &left_node = bound_set_op->left->Cast<BoundSelectNode>();
	auto &left_bind_state = left_node.bind_state;
	auto &right_node = bound_set_op->right->Cast<BoundSelectNode>();
	auto &right_bind_state = right_node.bind_state;

	// using set data structure to ensure uniqueness
	std::set<idx_t> collation_indexes(left_node.collation_sel_idx.begin(), left_node.collation_sel_idx.end());
	std::copy(right_node.collation_sel_idx.begin(), right_node.collation_sel_idx.end(), std::inserter(collation_indexes, collation_indexes.end()));

	// verifies collation conflicts
	for (idx_t collate_idx: collation_indexes) {
		auto &left_expr = left_bind_state.original_expressions[collate_idx];
		auto &right_expr = right_bind_state.original_expressions[collate_idx];
		// at least one expression must have to be a collation
		D_ASSERT(left_expr->GetExpressionClass() == ExpressionClass::COLLATE || right_expr->GetExpressionClass() == ExpressionClass::COLLATE);

		unique_ptr<Expression> bound_collation_expr;
		// collation on both sides
		if (left_expr->GetExpressionClass() == ExpressionClass::COLLATE && right_expr->GetExpressionClass() == ExpressionClass::COLLATE) {
			auto &left_collation_expr = left_expr->Cast<CollateExpression>();
			auto &right_collation_expr = right_expr->Cast<CollateExpression>();

			auto &left_str_collation = left_collation_expr.collation;
			auto &right_str_collation = right_collation_expr.collation;

			if (left_str_collation != right_str_collation) {
				throw BinderException("Different collations in a set operation at column: %lld.", collate_idx+1);
			}
			bound_collation_expr = left_node.select_list[collate_idx]->Copy();
		} else if (left_expr->GetExpressionClass() == ExpressionClass::COLLATE) {
			// collation on lhf
			bound_collation_expr = left_node.select_list[collate_idx]->Copy();
		} else {
			// collation on lhr
			bound_collation_expr = right_node.select_list[collate_idx]->Copy();
		}

		ExpressionBinder::PushCollation(context, bound_collation_expr, bound_collation_expr->return_type, true);
		bound_set_op->collation_group_info.push_back({collate_idx, std::move(bound_collation_expr)});
	}
}

unique_ptr<BoundQueryNode> Binder::BindNode(SetOperationNode &statement) {
	auto result = make_uniq<BoundSetOperationNode>();
	result->setop_type = statement.setop_type;
	result->setop_all = statement.setop_all;

	// first recursively visit the set operations
	// both the left and right sides have an independent BindContext and Binder
	D_ASSERT(statement.left);
	D_ASSERT(statement.right);

	result->setop_index = GenerateTableIndex();

	result->left_binder = Binder::CreateBinder(context, this);
	result->left_binder->can_contain_nulls = true;
	result->left = result->left_binder->BindNode(*statement.left);
	result->right_binder = Binder::CreateBinder(context, this);
	result->right_binder->can_contain_nulls = true;
	result->right = result->right_binder->BindNode(*statement.right);

	result->names = result->left->names;

	// move the correlated expressions from the child binders to this binder
	MoveCorrelatedExpressions(*result->left_binder);
	MoveCorrelatedExpressions(*result->right_binder);

	// now both sides have been bound we can resolve types
	if (result->setop_type != SetOperationType::UNION_BY_NAME &&
	    result->left->types.size() != result->right->types.size()) {
		throw BinderException("Set operations can only apply to expressions with the "
		                      "same number of result columns");
	}

	if (result->setop_type == SetOperationType::UNION_BY_NAME) {
		BuildUnionByNameInfo(context, *result, can_contain_nulls);
	} else {
		// figure out the types of the setop result by picking the max of both
		for (idx_t i = 0; i < result->left->types.size(); i++) {
			auto result_type = LogicalType::ForceMaxLogicalType(result->left->types[i], result->right->types[i]);
			if (!can_contain_nulls) {
				if (ExpressionBinder::ContainsNullType(result_type)) {
					result_type = ExpressionBinder::ExchangeNullType(result_type);
				}
			}
			result->types.push_back(result_type);
		}
	}

	SelectBindState bind_state;
	if (!statement.modifiers.empty()) {
		// handle the ORDER BY/DISTINCT clauses

		// we recursively visit the children of this node to extract aliases and expressions that can be referenced
		// in the ORDER BY

		if (result->setop_type == SetOperationType::UNION_BY_NAME) {
			GatherAliases(*result->left, bind_state, result->left_reorder_idx);
			GatherAliases(*result->right, bind_state, result->right_reorder_idx);
		} else {
			vector<idx_t> reorder_idx;
			for (idx_t i = 0; i < result->names.size(); i++) {
				reorder_idx.push_back(i);
			}
			GatherAliases(*result, bind_state, reorder_idx);
		}
		// now we perform the actual resolution of the ORDER BY/DISTINCT expressions
		OrderBinder order_binder({result->left_binder.get(), result->right_binder.get()}, bind_state);
		PrepareModifiers(order_binder, statement, *result);
	}

	// finally bind the types of the ORDER/DISTINCT clause expressions
	BindModifiers(*result, result->setop_index, result->names, result->types, bind_state);

	BindCollationGroup(result);

	return std::move(result);
}

} // namespace duckdb
