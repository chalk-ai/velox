/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/connectors/hive/storage_adapters/http/RegisterHttpFileSystem.h"

#include "velox/connectors/hive/storage_adapters/http/HttpFileSystem.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/FileSystems.h"

#include <gtest/gtest.h>

#include <unordered_map>

namespace facebook::velox::filesystems {
namespace {

class HttpFileSystemRegistrationTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    registerHttpFileSystem();
  }

  static void TearDownTestSuite() {
    finalizeHttpFileSystem();
  }
};

TEST_F(HttpFileSystemRegistrationTest, GetFileSystemFromRegistry) {
  auto config = std::make_shared<const config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});
  const std::string url{"http://example.com/data"};

  EXPECT_TRUE(isPathSupportedByRegisteredFileSystems(url));
  auto fs = filesystems::getFileSystem(url, config);
  ASSERT_NE(fs, nullptr);
  EXPECT_EQ(fs->name(), "http");
  EXPECT_NE(std::dynamic_pointer_cast<HttpFileSystem>(fs), nullptr);
}

TEST_F(HttpFileSystemRegistrationTest, CacheKeyUsesHost) {
  auto config = std::make_shared<const config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});

  auto baseHttp = filesystems::getFileSystem("http://example.com/a", config);
  auto otherPath = filesystems::getFileSystem("http://example.com/b", config);

  EXPECT_EQ(baseHttp, otherPath);
}

TEST_F(HttpFileSystemRegistrationTest, SupportsHttpsScheme) {
  auto config = std::make_shared<const config::ConfigBase>(
      std::unordered_map<std::string, std::string>{});
  const std::string url{"https://secure.example/data"};

  EXPECT_TRUE(isPathSupportedByRegisteredFileSystems(url));
  auto fs = filesystems::getFileSystem(url, config);
  ASSERT_NE(fs, nullptr);
  EXPECT_EQ(fs->name(), "http");
  EXPECT_NE(std::dynamic_pointer_cast<HttpFileSystem>(fs), nullptr);
}

} // namespace
} // namespace facebook::velox::filesystems
