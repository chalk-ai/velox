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

#include <memory>

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3FileSystem.h"
#include "velox/connectors/hive/storage_adapters/s3fs/S3Util.h"
#include "velox/connectors/hive/storage_adapters/s3fs/tests/MinioServer.h"

namespace facebook::velox::filesystems {
namespace {

// Exercises deletion through the configured S3 client against a local server.
class S3FileSystemRemoveTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    initializeS3("FATAL");
  }

  static void TearDownTestSuite() {
    finalizeS3();
  }

  void SetUp() override {
    minioServer_ = std::make_unique<MinioServer>();
    minioServer_->start();
  }

  void TearDown() override {
    minioServer_->stop();
  }

  std::unique_ptr<MinioServer> minioServer_;
};

TEST_F(S3FileSystemRemoveTest, removeAndRewrite) {
  const auto bucketName = "remove";
  minioServer_->addBucket(bucketName);
  const auto path = s3URI(bucketName, "preview/partition_0/preview_0.parquet");
  const auto sibling =
      s3URI(bucketName, "preview/partition_0/preview_1.parquet");
  S3FileSystem s3fs(bucketName, minioServer_->s3Config());
  auto pool = memory::memoryManager()->addLeafPool("removeAndRewrite");
  const FileOptions options{{}, pool.get(), std::nullopt};

  {
    auto writer = s3fs.openFileForWrite(path, options);
    writer->append("previous attempt");
    writer->close();
  }
  s3fs.mkdir(sibling);
  ASSERT_TRUE(s3fs.exists(path));

  s3fs.remove(path);
  EXPECT_FALSE(s3fs.exists(path));
  EXPECT_TRUE(s3fs.exists(sibling));
  // Cleanup also runs for first attempts and may be repeated after
  // interruption.
  EXPECT_NO_THROW(s3fs.remove(path));
  EXPECT_NO_THROW(s3fs.remove(s3URI(bucketName, "never-written")));

  {
    auto writer = s3fs.openFileForWrite(path, options);
    writer->append("retry");
    writer->close();
  }
  auto reader = s3fs.openFileForRead(path);
  EXPECT_EQ(reader->size(), 5);
  EXPECT_EQ(reader->pread(0, 5), "retry");
}

TEST_F(S3FileSystemRemoveTest, removePropagatesErrors) {
  const auto bucketName = "remove-errors";
  minioServer_->addBucket(bucketName);
  const auto path = s3URI(bucketName, "preview.parquet");
  S3FileSystem s3fs(bucketName, minioServer_->s3Config());
  s3fs.mkdir(path);

  S3FileSystem unauthorized(
      bucketName,
      minioServer_->s3Config({{"s3.aws-secret-key", "wrong-secret"}}));
  VELOX_ASSERT_THROW(
      unauthorized.remove(path), "Failed to delete object in S3");
  EXPECT_TRUE(s3fs.exists(path));
  VELOX_ASSERT_THROW(
      s3fs.remove(s3URI("missing-bucket", "preview.parquet")),
      "Failed to delete object in S3");
}

} // namespace
} // namespace facebook::velox::filesystems
