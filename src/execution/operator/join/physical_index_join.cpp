#include "duckdb/execution/operator/join/physical_index_join.hpp"
#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"

#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/types/constant_vector.hpp"
#include "duckdb/common/types/static_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/index_join.hpp"

using namespace std;

namespace duckdb {

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
    };

    class PhysicalIndexJoinState : public PhysicalComparisonIndexJoinState {
    public:
        PhysicalIndexJoinState(PhysicalOperator *left, vector<JoinCondition> &conditions)
                : PhysicalComparisonIndexJoinState(left, conditions), left_chunk(0), has_null(false), left_tuple(0),
                  right_tuple(0) {
        }

        index_t left_chunk;
        DataChunk right_join_condition;
        ChunkCollection left_data;
        ChunkCollection left_chunks;
        //! Whether or not the LHS of the nested loop join has NULL values
        bool has_null;

        index_t left_tuple;
        index_t right_tuple;
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
                for (index_t i = 0; i < state->left_chunks.chunks.size(); i++) {
                    state->has_null = state->has_null || PhysicalNestedLoopJoin::RemoveNullValues(*state->left_chunks.chunks[i]);
                }
                // initialize the chunks for the join conditions
                state->right_join_condition.Initialize(condition_types);
                state->left_chunk = state->left_chunks.chunks.size() - 1;
                state->left_tuple = state->left_chunks.chunks[state->left_chunk]->size();
            }
        }

        assert(state->left_chunks.count != 0);

        if (state->left_chunk >= state->left_chunks.chunks.size()) {
            return;
        }
        // now that we have fully materialized the LHS
        // we have to perform the nested loop join
        do {
            // first check if we have to move to the next child on the LHS
            assert(state->left_chunk < state->left_chunks.chunks.size());
            if (state->right_tuple >= state->left_chunks.chunks[state->left_chunk]->size()) {
                // we exhausted the chunk on the left
                state->left_chunk++;
//                if (state->left_chunk >= state->left_chunks.chunks.size()) {
//                    // we exhausted all LHS chunks!
//                    // move to the next left chunk
//                    do {
//                        children[0]->GetChunk(context, state->child_chunk, state->child_state.get());
//                        if (state->child_chunk.size() == 0) {
//                            return;
//                        }
//                        state->child_chunk.Flatten();
//
//                        // resolve the left join condition for the current chunk
//                        state->lhs_executor.Execute(state->child_chunk, state->left_join_condition);
//                        if (type != JoinType::MARK) {
//                            // immediately disqualify any tuples from the left side that have NULL values
//                            // we don't do this for the MARK join on the LHS, because the tuple will still be output, just
//                            // with a NULL marker!
//                            RemoveNullValues(state->left_join_condition);
//                        }
//                    } while (state->left_join_condition.size() == 0);
//
//                    state->right_chunk = 0;
//                }
                // move to the start of this chunk
                state->left_tuple = 0;
                state->right_tuple = 0;
            }

//            switch (type) {
//                case JoinType::SEMI:
//                case JoinType::ANTI:
//                case JoinType::MARK: {
//                    // MARK, SEMI and ANTI joins are handled separately because they scan the whole RHS in one go
//                    bool found_match[STANDARD_VECTOR_SIZE] = {false};
//                    NestedLoopJoinMark::Perform(state->left_join_condition, state->right_chunks, found_match, conditions);
//                    if (type == JoinType::MARK) {
//                        // now construct the mark join result from the found matches
//                        ConstructMarkJoinResult(state->left_join_condition, state->child_chunk, chunk, found_match,
//                                                state->has_null);
//                    } else if (type == JoinType::SEMI) {
//                        // construct the semi join result from the found matches
//                        ConstructSemiOrAntiJoinResult<true>(state->child_chunk, chunk, found_match);
//                    } else if (type == JoinType::ANTI) {
//                        ConstructSemiOrAntiJoinResult<false>(state->child_chunk, chunk, found_match);
//                    }
//                    // move to the next LHS chunk in the next iteration
//                    state->right_chunk = state->right_chunks.chunks.size();
//                    return;
//                }
//                default:
//                    break;
//            }

//            auto &left_chunk = state->child_chunk;
            auto &left_chunk = *state->left_chunks.chunks[state->left_chunk];
            auto &left_data = *state->left_data.chunks[state->left_chunk];

            // sanity check
            left_chunk.Verify();
            left_data.Verify();

            // now perform the join
            switch (type) {
                case JoinType::INNER: {
                    sel_t lvector[STANDARD_VECTOR_SIZE], rvector[STANDARD_VECTOR_SIZE];
                    index_t match_count =
                            IndexJoinInner::Perform(state->left_tuple, state->right_tuple, state->right_join_condition,
                                                         left_chunk, lvector, rvector, conditions);
                    // we have finished resolving the join conditions
                    if (match_count == 0) {
                        // if there are no results, move on
                        continue;
                    }
                    // we have matching tuples!
                    // construct the result
                    // create a reference to the chunk on the left side using the lvector
                    for (index_t i = 0; i < state->child_chunk.column_count; i++) {
                        chunk.data[i].Reference(state->child_chunk.data[i]);
                        chunk.data[i].count = match_count;
                        chunk.data[i].sel_vector = lvector;
                        chunk.data[i].Flatten();
                    }
                    // now create a reference to the chunk on the right side using the rvector
                    for (index_t i = 0; i < left_data.column_count; i++) {
                        index_t chunk_entry = state->child_chunk.column_count + i;
                        chunk.data[chunk_entry].Reference(left_data.data[i]);
                        chunk.data[chunk_entry].count = match_count;
                        chunk.data[chunk_entry].sel_vector = rvector;
                        chunk.data[chunk_entry].Flatten();
                    }
                    chunk.sel_vector = nullptr;
                    break;
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
