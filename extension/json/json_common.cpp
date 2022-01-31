#include "json_common.hpp"

namespace duckdb {

#define IDX_T_SAFE_DIG 19
#define IDX_T_MAX      ((idx_t)(~(idx_t)0))

/* only 0x0, 0x2E ('.'), and 0x5B ('[') are excluded from this table */
static const bool POINTER_CHAR_TABLE[] = {
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1};

static inline idx_t ReadString(const char *ptr, idx_t &len) {
	if (!POINTER_CHAR_TABLE[(uint8_t)*ptr]) {
		throw Exception("JSON path error");
	}
	const idx_t before = len;
	while (POINTER_CHAR_TABLE[(uint8_t)*ptr++]) {
		len--;
	}
	return before - len;
}

static inline bool ReadIndex(const char *ptr, idx_t &len, idx_t &idx, idx_t &i) {
	if (!POINTER_CHAR_TABLE[(uint8_t)*ptr]) {
		throw Exception("JSON path error");
	}
	idx = 0;
	for (i = 0; i < IDX_T_SAFE_DIG; i++) {
		const auto &c = *ptr++;
		len--;
		if (c == ']') {
			i++;
			return idx < (idx_t)IDX_T_MAX;
		} else if (len == 0) {
			break;
		}
		uint8_t add = (uint8_t)(c - '0');
		if (add <= 9) {
			idx = add + idx * 10;
		} else {
			break;
		}
	}
	throw Exception("JSON path error");
}

yyjson_val *JSONCommon::GetPointerDollar(yyjson_val *val, const char *ptr, idx_t len) {
	if (len == 1) {
		// Just '$'
		return val;
	}
	// Skip past '$'
	ptr++;
	len--;
	while (true) {
		const auto &c = *ptr++;
		len--;
		if (c == '.' && yyjson_is_obj(val)) {
			auto key_len = ReadString(ptr, len);
			val = yyjson_obj_getn(val, ptr, key_len);
			ptr += key_len;
		} else if (c == '[' && yyjson_is_arr(val)) {
			// We can index from back of array using #-<offset>
			bool offset_from_back = false;
			if (*ptr == '#') {
				ptr++;
				if (*ptr++ != '-') {
					return nullptr;
				}
				offset_from_back = true;
				len -= 2;
			}
			// Read index
			idx_t idx;
			idx_t index_len;
			if (ReadIndex(ptr, len, idx, index_len)) {
				if (offset_from_back) {
					auto arr_size = yyjson_arr_size(val);
					if (idx > arr_size) {
						return nullptr;
					}
					idx = arr_size - idx;
				}
				val = yyjson_arr_get(val, idx);
				ptr += index_len;
			} else {
				return nullptr;
			}
		} else {
			throw Exception("JSON path error");
		}
		if (len == 0) {
			return val;
		}
	}
}

} // namespace duckdb