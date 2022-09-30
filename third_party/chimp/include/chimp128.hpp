//===----------------------------------------------------------------------===//
//                         DuckDB
//
// third_party/chimp/include/chimp128.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <stddef.h>
#include <stdint.h>
#include "chimp_utils.hpp"
#include "leading_zero_buffer.hpp"
#include "flag_buffer.hpp"
#include "ring_buffer.hpp"
#include "duckdb/common/fast_mem.hpp"
#include "duckdb/common/likely.hpp"
#include "packed_data.hpp"

#include "byte_writer.hpp"
#include "byte_reader.hpp"

//#include "bit_reader_optimized.hpp"
//#include "output_bit_stream.hpp"

namespace duckdb_chimp {

enum CompressionFlags {
	VALUE_IDENTICAL = 0,
	TRAILING_EXCEEDS_THRESHOLD = 1,
	LEADING_ZERO_EQUALITY = 2,
	LEADING_ZERO_LOAD = 3
};

//===--------------------------------------------------------------------===//
// Compression
//===--------------------------------------------------------------------===//

template <bool EMPTY>
struct Chimp128CompressionState {

	Chimp128CompressionState() :
		ring_buffer(),
		previous_leading_zeros(std::numeric_limits<uint8_t>::max()) {}

	inline void SetLeadingZeros(int32_t value = std::numeric_limits<uint8_t>::max()) {
		this->previous_leading_zeros = value;
	}

	void Flush() {
		leading_zero_buffer.Flush();
	}

	void Reset() {
		first = true;
		ring_buffer.Reset();
		SetLeadingZeros();
		leading_zero_buffer.Reset();
		flag_buffer.Reset();
		packed_data_buffer.Reset();
	}

	ByteWriter<EMPTY>					output; //The stream to write to
	LeadingZeroBuffer<EMPTY>				leading_zero_buffer;
	FlagBuffer<EMPTY>						flag_buffer;
	PackedDataBuffer<EMPTY>					packed_data_buffer;
	RingBuffer								ring_buffer; //! The ring buffer that holds the previous values
	uint8_t									previous_leading_zeros; //! The leading zeros of the reference value
	bool									first = true;
};

template <bool EMPTY>
class Chimp128Compression {
public:
	using State = Chimp128CompressionState<EMPTY>;
	static constexpr uint8_t FLAG_MASK = (1 << 2) - 1;

	//this.previousValuesLog2 =  (int)(Math.log(previousValues) / Math.log(2));
	//! With 'previous_values' set to 128 this resolves to 7
	//! The amount of bits needed to store an index between 0-127
	static constexpr uint8_t INDEX_BITS_SIZE = 7;
	static constexpr uint8_t FLAG_BITS_SIZE = 2;
	static constexpr uint8_t FLAG_ZERO_SIZE = INDEX_BITS_SIZE + FLAG_BITS_SIZE;
	static constexpr uint8_t FLAG_ONE_SIZE = INDEX_BITS_SIZE + FLAG_BITS_SIZE + 9;
	static constexpr uint8_t BIT_SIZE = sizeof(uint64_t) * 8;

	//this.threshold = 6 + previousValuesLog2;
	static constexpr uint8_t TRAILING_ZERO_THRESHOLD = 6 + INDEX_BITS_SIZE;

	static void Store(uint64_t in, State& state) {
		if (state.first) {
			WriteFirst(in, state);
		}
		else {
			CompressValue(in, state);
		}
	}

	//! Write the content of the bit buffer to the stream
	static void Flush(State& state) {
		if (!EMPTY) {
			state.output.Flush();
		}
	}

	static void WriteFirst(uint64_t in, State& state) {
		state.ring_buffer.template Insert<true>(in);
		state.output.template WriteValue<uint64_t, BIT_SIZE>(in);
		state.first = false;
	}

	static void CompressValue(uint64_t in, State& state) {
		static constexpr uint8_t LEADING_REPRESENTATION[] = {
			0, 0, 0, 0, 0, 0, 0, 0,
			1, 1, 1, 1, 2, 2, 2, 2,
			3, 3, 4, 4, 5, 5, 6, 6,
			7, 7, 7, 7, 7, 7, 7, 7,
			7, 7, 7, 7, 7, 7, 7, 7,
			7, 7, 7, 7, 7, 7, 7, 7,
			7, 7, 7, 7, 7, 7, 7, 7,
			7, 7, 7, 7, 7, 7, 7, 7
		};

		auto key = state.ring_buffer.Key(in);
		uint64_t xor_result;
		uint8_t previous_index;
		uint32_t trailing_zeros = 0;
		bool trailing_zeros_exceed_threshold = false;

		//! Find the reference value to use when compressing the current value
		if (((int64_t)state.ring_buffer.Size() - (int64_t)key) < (int64_t)RingBuffer::RING_SIZE) {
			auto current_index = state.ring_buffer.IndexOf(key);
			if (current_index > state.ring_buffer.Size()) {
				current_index = 0;
			}
			auto reference_value = state.ring_buffer.Value(current_index % RingBuffer::RING_SIZE);
			uint64_t tempxor_result = (uint64_t)in ^ reference_value;
			trailing_zeros = __builtin_ctzll(tempxor_result);
			trailing_zeros_exceed_threshold = trailing_zeros > TRAILING_ZERO_THRESHOLD;
			if (trailing_zeros_exceed_threshold) {
				previous_index = current_index % RingBuffer::RING_SIZE;
				xor_result = tempxor_result;
			}
			else {
				previous_index = state.ring_buffer.Size() % RingBuffer::RING_SIZE;
				xor_result = (uint64_t)in ^ state.ring_buffer.Value(previous_index);
			}
		}
		else {
			previous_index = state.ring_buffer.Size() % RingBuffer::RING_SIZE;
			xor_result = (uint64_t)in ^ state.ring_buffer.Value(previous_index);
		}


		//! Compress the value
		if (xor_result == 0) {
			state.flag_buffer.Insert(VALUE_IDENTICAL);
			//! The two values are identical (write 9 bits)
			//! 2 bits for the flag VALUE_IDENTICAL ('00') + 7 bits for the referenced index value
			state.output.template WriteValue<uint8_t, FLAG_ZERO_SIZE - FLAG_BITS_SIZE>(previous_index);
			state.SetLeadingZeros();
		}
		else {
			//! Values are not identical (64)
			auto leading_zeros_raw = __builtin_clzll(xor_result);
			uint8_t leading_zeros = ChimpCompressionConstants::LEADING_ROUND[leading_zeros_raw];

			if (trailing_zeros_exceed_threshold) {
				state.flag_buffer.Insert(TRAILING_EXCEEDS_THRESHOLD);
				//! write (64 - [0|8|12|16|18|20|22|24] - [14+])(26-50 bits) and 18 bits
				uint32_t significant_bits = BIT_SIZE - leading_zeros - trailing_zeros;
				//! FIXME: it feels like this would produce '11', indicating LEADING_ZERO_LOAD
				//! Instead of indicating TRAILING_EXCEEDS_THRESHOLD '01'
				auto result = 512U * (RingBuffer::RING_SIZE + previous_index) + BIT_SIZE * LEADING_REPRESENTATION[leading_zeros] + significant_bits;
				state.packed_data_buffer.Insert(result & 0xFFFF);
				//state.output.template WriteValue<uint16_t, 16>((uint16_t)(result & 0xFFFF));
				state.output.template WriteValue<uint64_t>(xor_result >> trailing_zeros, significant_bits);
				state.SetLeadingZeros();
			}
			else if (leading_zeros == state.previous_leading_zeros) {
				state.flag_buffer.Insert(LEADING_ZERO_EQUALITY);
				//! write 2 + [?] bits
				int32_t significant_bits = BIT_SIZE - leading_zeros;
				state.output.template WriteValue<uint64_t>(xor_result, significant_bits);
			}
			else {
				state.flag_buffer.Insert(LEADING_ZERO_LOAD);
				const int32_t significant_bits = BIT_SIZE - leading_zeros;
				//! 2 bits for the flag LEADING_ZERO_LOAD ('11') + 3 bits for the leading zeros
				state.leading_zero_buffer.Insert(LEADING_REPRESENTATION[leading_zeros]);
				state.output.template WriteValue<uint64_t>(xor_result, significant_bits);
				state.SetLeadingZeros(leading_zeros);
			}
		}
		// Byte-align every value we write to the output
		state.ring_buffer.Insert(in);
	}
};

//===--------------------------------------------------------------------===//
// Decompression
//===--------------------------------------------------------------------===//

struct Chimp128DecompressionState {
public:
	Chimp128DecompressionState() :
		reference_value(0),
		first(true)
	{
		ResetZeros();
	}

	void Reset() {
		ResetZeros();
		reference_value = 0;
		ring_buffer.Reset();
		first = true;
	}

	inline void ResetZeros() {
		leading_zeros = std::numeric_limits<uint8_t>::max();
		trailing_zeros = 0;
	}

	inline void SetLeadingZeros(uint8_t value) {
		leading_zeros = value;
	}

	inline void SetTrailingZeros(uint8_t value) {
		assert(value <= sizeof(uint64_t) * 8);
		trailing_zeros = value;
	}

	uint8_t LeadingZeros() const {
		return leading_zeros;
	}
	uint8_t TrailingZeros() const {
		return trailing_zeros;
	}

	ByteReader 					input;
	//LeadingZeroBuffer<false>	leading_zero_buffer;
	//FlagBuffer<false>			flag_buffer;
	uint8_t 					leading_zeros;
	uint8_t 					trailing_zeros;
	uint64_t 					reference_value = 0;
	RingBuffer					ring_buffer;

	bool first;
};

template <class RETURN_TYPE>
struct Chimp128Decompression {
public:
	//! Index value is between 1 and 127, so it's saved in 7 bits at most
	static constexpr uint8_t INDEX_BITS_SIZE = 7;
	static constexpr uint8_t LEADING_BITS_SIZE = 3;
	static constexpr uint8_t SIGNIFICANT_BITS_SIZE = 6;
	static constexpr uint8_t INITIAL_FILL = INDEX_BITS_SIZE + LEADING_BITS_SIZE + SIGNIFICANT_BITS_SIZE;
	static constexpr uint8_t BIT_SIZE = sizeof(uint64_t) * 8;


	static inline void UnpackPackedData(uint16_t packed_data, UnpackedData& dest) {
		return PackedDataUtils::Unpack(packed_data, dest);
	}

	static inline RETURN_TYPE Load(uint8_t flag, uint8_t leading_zeros[], uint32_t &leading_zero_index,
	   UnpackedData unpacked_data[], uint32_t& unpacked_index, Chimp128DecompressionState& state) {
		if (DUCKDB_UNLIKELY(state.first)) {
			return LoadFirst(state);
		}
		else {
			return DecompressValue(flag, leading_zeros, leading_zero_index, unpacked_data, unpacked_index, state);
		}
	}

	static inline RETURN_TYPE LoadFirst(Chimp128DecompressionState& state) {
		RETURN_TYPE result = state.input.template ReadValue<RETURN_TYPE, sizeof(RETURN_TYPE) * __CHAR_BIT__>();
		state.ring_buffer.InsertScan<true>(result);
		state.first = false;
		state.reference_value = result;
		return result;
	}

	static inline RETURN_TYPE DecompressValue(uint8_t flag, uint8_t leading_zeros[],
	   uint32_t &leading_zero_index, UnpackedData unpacked_data[], uint32_t& unpacked_index, 
	   Chimp128DecompressionState& state) {
		static const constexpr uint8_t LEADING_REPRESENTATION[] = {
			0, 8, 12, 16, 18, 20, 22, 24
		};
		//static thread_local uint64_t flag_count[4] = {0};
		//flag_count[flag]++;
		//printf("FLAG[%u] - total_count: %llu\n", (uint32_t)flag, (uint64_t)flag_count[flag]);

		RETURN_TYPE result;
		switch (flag) {
		case VALUE_IDENTICAL: {
			//! Value is identical to previous value
			auto index = state.input.template ReadValue<uint8_t, 7>();
			result = state.ring_buffer.Value(index);
			break;
		}
		case TRAILING_EXCEEDS_THRESHOLD: {
			const UnpackedData &unpacked = unpacked_data[unpacked_index++];
			//const uint16_t temp = state.input.template ReadValue<uint16_t, sizeof(uint16_t) * __CHAR_BIT__>();
			//UnpackPackedData(temp, unpacked);
			state.leading_zeros = LEADING_REPRESENTATION[unpacked.leading_zero];
			state.trailing_zeros = BIT_SIZE - unpacked.significant_bits - state.leading_zeros;
			//static thread_local uint64_t total_bitcount_read = 0;
			//static thread_local uint64_t bits_to_read_count[65] = {0};
			//static thread_local uint64_t total_read_count = 0;
			//bits_to_read_count[bits_to_read]++;
			//total_bitcount_read += bits_to_read;
			//total_read_count++;
			//printf("BITS_READ[%u] - count: %llu\n", (uint32_t)bits_to_read, bits_to_read_count[bits_to_read]);
			//printf("[%llu] - TOTAL BITS READ: %llu\n", total_read_count, total_bitcount_read);
			result = state.input.template ReadValue<uint64_t>(unpacked.significant_bits);
			result <<= state.trailing_zeros;
			result ^= state.ring_buffer.Value(unpacked.index);
			break;
		}
		case LEADING_ZERO_EQUALITY: {
			result = state.input.template ReadValue<uint64_t>(BIT_SIZE - state.leading_zeros);
			result ^= state.reference_value;
			break;
		}
		case LEADING_ZERO_LOAD: {
			state.leading_zeros = leading_zeros[leading_zero_index++];
			result = state.input.template ReadValue<uint64_t>(BIT_SIZE - state.leading_zeros);
			result ^= state.reference_value;
			break;
		}
		default:
			throw std::runtime_error("eek");
		}
		state.reference_value = result;
		state.ring_buffer.InsertScan(result);
		return result;
	}
};

} //namespace duckdb_chimp
