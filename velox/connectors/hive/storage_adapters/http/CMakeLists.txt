velox_add_library(velox_httpfs RegisterHttpFileSystem.cpp)
if(VELOX_ENABLE_HTTP)
    find_package(CURL REQUIRED)

    velox_sources(
            velox_httpfs
            PRIVATE HttpFileSystem.cpp
    )

    velox_link_libraries(velox_httpfs PRIVATE velox_dwio_common Folly::folly CURL::libcurl)

    if(${VELOX_BUILD_TESTING})
        add_subdirectory(tests)
    endif()
endif()
