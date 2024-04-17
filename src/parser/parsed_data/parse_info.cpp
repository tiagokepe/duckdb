#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/enums/catalog_type.hpp"

namespace duckdb {

string ParseInfo::TypeToString(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		return "TABLE";
	case CatalogType::SCALAR_FUNCTION_ENTRY:
		return "FUNCTION";
	case CatalogType::INDEX_ENTRY:
		return "INDEX";
	case CatalogType::SCHEMA_ENTRY:
		return "SCHEMA";
	case CatalogType::TYPE_ENTRY:
		return "TYPE";
	case CatalogType::VIEW_ENTRY:
		return "VIEW";
	case CatalogType::SEQUENCE_ENTRY:
		return "SEQUENCE";
	case CatalogType::MACRO_ENTRY:
		return "MACRO";
	case CatalogType::TABLE_MACRO_ENTRY:
		return "MACRO TABLE";
	default:
		throw InternalException("ParseInfo::TypeToString for CatalogType with type: %s not implemented",
		                        EnumUtil::ToString(type));
	}
}

} // namespace duckdb
