#include "duckdb/execution/index_join.hpp"

#include "duckdb/common/operator/comparison_operators.hpp"

//#include "duckdb/execution/index/art/art.hpp"

using namespace duckdb;
using namespace std;

struct InitialIndexJoin {
	template <class T, class OP>
	static index_t Operation(DataChunk &result_chunk, Vector &left, index_t &lpos, index_t current_match_count,
	                         TableCatalogEntry &tableref, DataTable &table, Index &index, vector<column_t> column_ids) {
		//! initialize phase of nested loop join
		auto ldata = (T *)left.GetData();
		index_t result_count = 0;
		for (; lpos < left.count; lpos++) {
			if (result_count == STANDARD_VECTOR_SIZE) {
				//! out of space!
				return result_count;
			}
			index_t left_position = left.sel_vector ? left.sel_vector[lpos] : lpos;
			assert(!left.nullmask[left_position]);
			//! Perform Index Lookup
			//! If we find a match emit tuples
			auto x = ldata[left_position];
			int ha = 10;
			//            unique_ptr<Key> key = ART::CreateKey((ART &) index, left.type,Value(ldata[left_position]));
			//            auto leaf = static_cast<Leaf *>(Lookup(tree, *key, 0));
			//            if (!leaf) {
			//                return;
			//            }
			//            for (index_t i = 0; i < leaf->num_elements; i++) {
			//                row_t row_id = leaf->GetRowId(i);
			//                result_ids.push_back(row_id);
			//            }
			//				if (OP::Operation(ldata[left_position], rdata[right_position])) {
			//					// emit tuple
			//					lvector[result_count] = left_position;
			//					rvector[result_count] = right_position;
			//					result_count++;
			//				}
		}
		if (result_count > 0) {
			//            //! we have matching tuples!
			//            //! construct the result
			//            //! create a reference to the chunk on the left side using the lvector
			//            for (index_t i = 0; i < state->child_chunk.column_count; i++) {
			//                chunk.data[i].Reference(state->child_chunk.data[i]);
			//                chunk.data[i].count = match_count;
			//                chunk.data[i].sel_vector = lvector;
			//                chunk.data[i].Flatten();
			//            }
			//            //! now create a reference to the chunk on the right side using the rvector
			//            for (index_t i = 0; i < left_data.column_count; i++) {
			//                index_t chunk_entry = state->child_chunk.column_count + i;
			//                chunk.data[chunk_entry].Reference(left_data.data[i]);
			//                chunk.data[chunk_entry].count = match_count;
			//                chunk.data[chunk_entry].sel_vector = rvector;
			//                chunk.data[chunk_entry].Flatten();
			//            }
			//            chunk.sel_vector = nullptr;
			//            break;
			//        }
			return result_count;
		}
	}
};

	template <class NLTYPE, class OP>
	static index_t index_join_inner_operator(DataChunk &result_chunk, Vector &left, index_t &lpos,
	                                         index_t current_match_count, TableCatalogEntry &tableref, DataTable &table,
	                                         Index &index, vector<column_t> column_ids) {
		switch (left.type) {
			//	case TypeId::BOOL:
			//	case TypeId::INT8:
			//		return NLTYPE::template Operation<int8_t, OP>(left, lpos, lvector, current_match_count,tableref,table,index,column_ids); 	case TypeId::INT16: 		return NLTYPE::template Operation<int16_t, OP>(left, lpos, lvector, current_match_count,tableref,table,index,column_ids);
		case TypeId::INT32:
			return NLTYPE::template Operation<int32_t, OP>(result_chunk, left, lpos, current_match_count, tableref,
			                                               table, index, column_ids);
			//	case TypeId::INT64:
			//		return NLTYPE::template Operation<int64_t, OP>(left, lpos, lvector, current_match_count,tableref,table,index,column_ids); 	case TypeId::FLOAT: 		return NLTYPE::template Operation<float, OP>(left, right, lpos, rpos, lvector, rvector, current_match_count,tableref,table,index,column_ids); 	case TypeId::DOUBLE: 		return NLTYPE::template Operation<double, OP>(left, right, lpos, rpos, lvector, rvector, current_match_count,tableref,table,index,column_ids); 	case TypeId::VARCHAR: 		return NLTYPE::template Operation<const char *, OP>(left, right, lpos, rpos, lvector, rvector,
			//		                                                    current_match_count,tableref,table,index,column_ids);
		default:
			throw NotImplementedException("Unimplemented type for index join!");
		}
	}

	template <class NLTYPE>
	index_t index_join_inner(DataChunk &result_chunk, Vector &left, index_t &lpos, index_t current_match_count,
	                         ExpressionType comparison_type, TableCatalogEntry &tableref, DataTable &table,
	                         Index &index, vector<column_t> column_ids) {
		switch (comparison_type) {
		case ExpressionType::COMPARE_EQUAL:
			return index_join_inner_operator<NLTYPE, duckdb::Equals>(result_chunk, left, lpos, current_match_count,
			                                                         tableref, table, index, column_ids);
		default:
			throw NotImplementedException("Unimplemented comparison type for index join!");
		}
	}

	index_t IndexJoinInner::Perform(DataChunk &result_chunk, index_t &lpos, DataChunk &left_conditions,
	                vector<JoinCondition> &conditions, TableCatalogEntry &tableref, DataTable &table, Index &index,
	                vector<column_t> column_ids) {
		index_t current_match_count = 0;
		index_t match_count =
		    index_join_inner<InitialIndexJoin>(result_chunk, left_conditions.data[0], current_match_count, lpos,
		                                       conditions[0].comparison, tableref, table, index, column_ids);
		// TODO: We only use index joins with one condition
		assert(conditions.size() == 1);
		return match_count;
	}
