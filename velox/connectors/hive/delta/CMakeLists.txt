velox_add_library(
        velox_hive_delta_splitreader
        DeltaSplitReader.cpp
)

velox_link_libraries(velox_hive_delta_splitreader velox_connector Folly::folly)

#if(${VELOX_BUILD_TESTING})
#    add_subdirectory(tests)
#endif()
