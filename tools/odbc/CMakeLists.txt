include(FindODBC)

IF(ODBC_FOUND)
	add_definitions(-DHAVE_SQLLEN -DHAVE_SQLULEN -DSETSTMTOPTION_LAST_ARG_TYPE=SQLULEN -DCOLATTRIBUTE_LAST_ARG_TYPE=SQLLEN* -DWITHOUT_DRIVERMGR -DHAVE_SQLSETPOSIROW)
	
	include_directories(.)
	include_directories(${ODBC_INCLUDE_DIRS})
	include_directories(../sqlite3_api_wrapper/include)
	
	add_library(duckdb_odbc SHARED sqlite3odbc.c)
	target_link_libraries(duckdb_odbc sqlite3_api_wrapper_static)
ENDIF()