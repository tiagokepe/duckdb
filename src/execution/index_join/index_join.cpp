#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/execution/index_join.hpp"

using namespace duckdb;
using namespace std;

struct InitialIndexJoin {
	template <class T, class OP>
	static index_t Operation(Vector &left, Vector &right, index_t &lpos, index_t &rpos, sel_t lvector[],
	                         sel_t rvector[], index_t current_match_count) {
		// initialize phase of nested loop join
		// fill lvector and rvector with matches from the base vectors
		auto ldata = (T *)left.data;
		auto rdata = (T *)right.data;
		index_t result_count = 0;
		for (; rpos < right.count; rpos++) {
			index_t right_position = right.sel_vector ? right.sel_vector[rpos] : rpos;
			assert(!right.nullmask[right_position]);
			for (; lpos < left.count; lpos++) {
				if (result_count == STANDARD_VECTOR_SIZE) {
					// out of space!
					return result_count;
				}
				index_t left_position = left.sel_vector ? left.sel_vector[lpos] : lpos;
				assert(!left.nullmask[left_position]);
				if (OP::Operation(ldata[left_position], rdata[right_position])) {
					// emit tuple
					lvector[result_count] = left_position;
					rvector[result_count] = right_position;
					result_count++;
				}
			}
			lpos = 0;
		}
		return result_count;
	}
};


template <class NLTYPE, class OP>
static index_t index_join_inner_operator(Vector &left, Vector &right, index_t &lpos, index_t &rpos,
                                               sel_t lvector[], sel_t rvector[], index_t current_match_count) {
	switch (left.type) {
	case TypeId::BOOLEAN:
	case TypeId::TINYINT:
		return NLTYPE::template Operation<int8_t, OP>(left, right, lpos, rpos, lvector, rvector, current_match_count);
	case TypeId::SMALLINT:
		return NLTYPE::template Operation<int16_t, OP>(left, right, lpos, rpos, lvector, rvector, current_match_count);
	case TypeId::INTEGER:
		return NLTYPE::template Operation<int32_t, OP>(left, right, lpos, rpos, lvector, rvector, current_match_count);
	case TypeId::BIGINT:
		return NLTYPE::template Operation<int64_t, OP>(left, right, lpos, rpos, lvector, rvector, current_match_count);
	case TypeId::FLOAT:
		return NLTYPE::template Operation<float, OP>(left, right, lpos, rpos, lvector, rvector, current_match_count);
	case TypeId::DOUBLE:
		return NLTYPE::template Operation<double, OP>(left, right, lpos, rpos, lvector, rvector, current_match_count);
	case TypeId::VARCHAR:
		return NLTYPE::template Operation<const char *, OP>(left, right, lpos, rpos, lvector, rvector,
		                                                    current_match_count);
	default:
		throw NotImplementedException("Unimplemented type for index join!");
	}
}

template <class NLTYPE>
index_t index_join_inner(Vector &left, Vector &right, index_t &lpos, index_t &rpos, sel_t lvector[],
                               sel_t rvector[], index_t current_match_count, ExpressionType comparison_type) {
	assert(left.type == right.type);
	switch (comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		return index_join_inner_operator<NLTYPE, duckdb::Equals>(left, right, lpos, rpos, lvector, rvector,
		                                                               current_match_count);
	default:
		throw NotImplementedException("Unimplemented comparison type for index join!");
	}
}

index_t IndexJoinInner::Perform(index_t &lpos, index_t &rpos, DataChunk &left_conditions,
                                     DataChunk &right_conditions, sel_t lvector[], sel_t rvector[],
                                     vector<JoinCondition> &conditions) {
	assert(left_conditions.column_count == right_conditions.column_count);
	if (lpos >= left_conditions.size() || rpos >= right_conditions.size()) {
		return 0;
	}
	index_t match_count = index_join_inner<InitialIndexJoin>(
	    left_conditions.data[0], right_conditions.data[0], lpos, rpos, lvector, rvector, 0, conditions[0].comparison);
	// TODO: We only use index joins with one condition
	assert(conditions.size() == 1);
	return match_count;
}
