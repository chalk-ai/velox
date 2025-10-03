add_executable(
  velox_httpfile_test
  HttpFileSystemTest.cpp
  HttpFileSystemRegistrationTest.cpp)
add_test(velox_httpfile_test velox_httpfile_test)
target_link_libraries(
  velox_httpfile_test
  velox_httpfs
  velox_file
  Folly::folly
  fmt::fmt
  GTest::gtest
  GTest::gtest_main
)
