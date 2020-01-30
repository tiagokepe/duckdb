#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"
#include "duckdb/execution/index/art/art_key.hpp"

#include <cfloat>
#include <iostream>

using namespace duckdb;
using namespace std;

TEST_CASE("Test Index Join", "[index-join]") {
	DuckDB db(nullptr);
	Connection con(db);
	unique_ptr<QueryResult> result;
	con.EnableQueryVerification();

	// create tables
	con.Query("CREATE TABLE test (a INTEGER, b INTEGER);");
	con.Query("INSERT INTO test VALUES (11, 1), (12, 2), (13, 3)");

	con.Query("CREATE TABLE test2 (b INTEGER, c INTEGER);");
	con.Query("INSERT INTO test2 VALUES (1, 10), (1, 20), (2, 30)");
	con.Query("CREATE INDEX i_index on test(b)");
	SECTION("simple cross product + join condition") {
		//        PROJECTION[a, b, c]
		//          COMPARISON_JOIN[INNER EQUAL(b, b)]
		//              GET(test)
		//              GET(test2)
		result = con.Query("SELECT a, test.b, c "
		                   "FROM test, test2 "
		                   "WHERE test.b = test2.b ORDER BY c;");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 11, 12}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 1, 2}));
		REQUIRE(CHECK_COLUMN(result, 2, {10, 20, 30}));
	}
}
