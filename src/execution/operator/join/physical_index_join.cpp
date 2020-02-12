#include "duckdb/execution/operator/join/physical_index_join.hpp"

#include "duckdb/common/operator/comparison_operators.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index_join.hpp"

using namespace std;

namespace duckdb {
//
    class PhysicalComparisonIndexJoinState : public PhysicalOperatorState {
    public:
        PhysicalComparisonIndexJoinState(PhysicalOperator *left, vector<JoinCondition> &conditions)
                : PhysicalOperatorState(left) {
            assert(left);
            for (auto &cond : conditions) {
                lhs_executor.AddExpression(*cond.left);
            }
        }
        ExpressionExecutor lhs_executor;
        bool initialized;
        TableIndexScanState scan_state;
    };

    class PhysicalIndexJoinState : public PhysicalComparisonIndexJoinState {
    public:
        PhysicalIndexJoinState(PhysicalOperator *left, vector<JoinCondition> &conditions)
                : PhysicalComparisonIndexJoinState(left, conditions), left_chunk(0), has_null(false), left_tuple(0) {
        }

        index_t left_chunk;
        ChunkCollection left_data;
        ChunkCollection left_chunks;
        //! Whether or not the LHS of the nested loop join has NULL values
        bool has_null;
        index_t left_tuple;
    };

    void PhysicalIndexJoin::GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state_) {
        auto state = reinterpret_cast<PhysicalIndexJoinState *>(state_);

        // first we fully materialize the left child, if we haven't done that yet
        if (state->left_chunks.column_count() == 0) {
            vector<TypeId> condition_types;
            for (auto &cond : conditions) {
                assert(cond.left->return_type == cond.right->return_type);
                condition_types.push_back(cond.left->return_type);
            }

            auto left_state = children[0]->GetOperatorState();
            auto types = children[0]->GetTypes();

            DataChunk new_chunk, left_condition;
            new_chunk.Initialize(types);
            left_condition.Initialize(condition_types);
            do {
                children[0]->GetChunk(context, new_chunk, left_state.get());
                if (new_chunk.size() == 0) {
                    break;
                }
                // resolve the join expression of the left side
                state->lhs_executor.Execute(new_chunk, left_condition);

                state->left_data.Append(new_chunk);
                state->left_chunks.Append(left_condition);
            } while (new_chunk.size() > 0);

            if (state->left_chunks.count == 0) {
                if ((type == JoinType::INNER || type == JoinType::SEMI)) {
                    // empty RHS with INNER or SEMI join means empty result set
                    return;
                }
            } else {
                // disqualify tuples from the LHS that have NULL values
//                for (index_t i = 0; i < state->left_chunks.chunks.size(); i++) {
//                    state->has_null = state->has_null || PhysicalNestedLoopJoin::RemoveNullValues(*state->left_chunks.chunks[i]);
//                }
                state->left_chunk = state->left_chunks.chunks.size() - 1;
                state->left_tuple = state->left_chunks.chunks[state->left_chunk]->size();
            }
        }

        assert(state->left_chunks.count != 0);

        if (state->left_chunk >= state->left_chunks.chunks.size()) {
            return;
        }
        // now that we have fully materialized the LHS
        // we have to perform the index join
        do {
            // first check if we have to move to the next child on the LHS
            assert(state->left_chunk < state->left_chunks.chunks.size());
            if (state->left_tuple >= state->left_chunks.chunks[state->left_chunk]->size()) {
                // we exhausted the chunk on the left
                state->left_chunk++;
                // move to the start of this chunk
                state->left_tuple = 0;
            }

            auto &left_chunk = *state->left_chunks.chunks[state->left_chunk];
            auto &left_data = *state->left_data.chunks[state->left_chunk];

            // sanity check
            left_chunk.Verify();
            left_data.Verify();

            // now perform the join
            switch (type) {
                case JoinType::INNER: {
//                    sel_t lvector[STANDARD_VECTOR_SIZE], rvector[STANDARD_VECTOR_SIZE];
                    index_t match_count =
                            IndexJoinInner::Perform(chunk,state->left_tuple, left_chunk, conditions,tableref,table,index,column_ids);
                    // we have finished resolving the join conditions
                    if (match_count == 0) {
                        // if there are no results, move on
                        continue;
                    }
                }
                default:
                    throw NotImplementedException("Unimplemented type for index join!");
            }
        } while (chunk.size() == 0);
    }

    unique_ptr<PhysicalOperatorState> PhysicalIndexJoin::GetOperatorState() {
        return make_unique<PhysicalIndexJoinState>(children[0].get(), conditions);
    }

} // namespace duckdb
