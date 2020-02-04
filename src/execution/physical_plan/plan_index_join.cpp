#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_index_join.hpp"
#include "duckdb/execution/operator/join/physical_index_join.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalIndexJoin &op) {
    //! now visit the only child (LHS)
    assert(op.children.size() == 1);
    auto left = CreatePlan(*op.children[0]);
    assert(left);
    return make_unique<PhysicalIndexJoin>(op, move(left), move(op.conditions), op.join_type,op.tableref, op.table, op.index, op.column_ids);

}
