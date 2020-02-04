add_library_unity(duckdb_index_join
                  OBJECT
        index_join.cpp
                 )
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_index_join> PARENT_SCOPE)
