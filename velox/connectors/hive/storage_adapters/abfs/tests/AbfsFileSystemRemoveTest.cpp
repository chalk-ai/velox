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

#include <azure/core/context.hpp>
#include <azure/core/http/http.hpp>
#include <azure/core/http/raw_response.hpp>
#include <azure/core/http/transport.hpp>
#include <azure/core/url.hpp>
#include <azure/storage/files/datalake/datalake_file_client.hpp>
#include <azure/storage/files/datalake/datalake_options.hpp>
#include <gtest/gtest.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/File.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsFileSystem.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsPath.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureBlobClient.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureClientProvider.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureClientProviderFactories.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureClientProviderImpl.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureDataLakeFileClient.h"
#include "velox/connectors/hive/storage_adapters/abfs/DynamicSasTokenClientProvider.h"
#include "velox/connectors/hive/storage_adapters/abfs/tests/MockDataLakeFileClient.h"

namespace facebook::velox::filesystems {
namespace {

// Keeps write and cleanup operations on the same local file backend.
class LocalAzureClientProvider final : public AzureClientProvider {
 public:
  explicit LocalAzureClientProvider(std::string directory)
      : directory_(std::move(directory)) {}

  std::unique_ptr<AzureBlobClient> getReadFileClient(
      const std::shared_ptr<AbfsPath>&,
      const config::ConfigBase&) override {
    VELOX_FAIL("Unexpected read client request.");
  }

  std::unique_ptr<AzureDataLakeFileClient> getWriteFileClient(
      const std::shared_ptr<AbfsPath>& path,
      const config::ConfigBase& config) override {
    EXPECT_EQ(
        config.get<std::string>("remove-test-marker").value(), "configured");
    return std::make_unique<MockDataLakeFileClient>(
        directory_ + "/" + path->filePath());
  }

 private:
  const std::string directory_;
};

TEST(AbfsFileSystemRemoveTest, removeAndRewrite) {
  auto directory = common::testutil::TempDirectoryPath::create();
  AzureClientProviderFactories::registerFactory(
      "remove-test", [directory](const std::string&) {
        return std::make_unique<LocalAzureClientProvider>(directory->getPath());
      });
  AbfsFileSystem filesystem(
      std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>{
              {"remove-test-marker", "configured"}}));
  for (const auto& scheme : {"abfs", "abfss"}) {
    const auto prefix =
        std::string(scheme) + "://container@remove-test.dfs.core.windows.net/";
    const auto path = prefix + "preview_0.parquet";
    const auto sibling = prefix + "preview_1.parquet";
    for (const auto& output : {path, sibling}) {
      auto writer = filesystem.openFileForWrite(output);
      writer->append("previous attempt");
      writer->close();
    }
    VELOX_ASSERT_THROW(
        filesystem.openFileForWrite(path), "File already exists");
    filesystem.remove(path);
    EXPECT_NO_THROW(filesystem.remove(path));
    EXPECT_NO_THROW(filesystem.remove(prefix + "never-written"));
    auto writer = filesystem.openFileForWrite(path);
    writer->append("retry");
    writer->close();
    MockDataLakeFileClient output(directory->getPath() + "/preview_0.parquet");
    EXPECT_EQ(output.readContent(), "retry");
    MockDataLakeFileClient neighbor(
        directory->getPath() + "/preview_1.parquet");
    EXPECT_EQ(neighbor.readContent(), "previous attempt");
    filesystem.remove(path);
    filesystem.remove(sibling);
  }
}

// Exercises the SDK's DELETE handling without a cloud account or emulator.
class DeleteTransport final : public Azure::Core::Http::HttpTransport {
 public:
  DeleteTransport(
      Azure::Core::Http::HttpStatusCode status,
      std::string errorCode)
      : status_(status), errorCode_(std::move(errorCode)) {}

  std::unique_ptr<Azure::Core::Http::RawResponse> Send(
      Azure::Core::Http::Request& request,
      const Azure::Core::Context&) override {
    EXPECT_EQ(request.GetMethod(), Azure::Core::Http::HttpMethod::Delete);
    EXPECT_EQ(
        request.GetUrl().GetAbsoluteUrl(),
        "https://remove-sdk.dfs.core.windows.net/container/preview_0.parquet");
    ++requests;
    auto response = std::make_unique<Azure::Core::Http::RawResponse>(
        1, 1, status_, "test response");
    response->SetHeader("content-length", "0");
    if (!errorCode_.empty()) {
      response->SetHeader("x-ms-error-code", errorCode_);
    }
    return response;
  }

  int requests{0};

 private:
  const Azure::Core::Http::HttpStatusCode status_;
  const std::string errorCode_;
};

class SdkAzureClientProvider final : public AzureClientProvider {
 public:
  explicit SdkAzureClientProvider(std::shared_ptr<DeleteTransport> transport)
      : transport_(std::move(transport)) {}

  std::unique_ptr<AzureBlobClient> getReadFileClient(
      const std::shared_ptr<AbfsPath>&,
      const config::ConfigBase&) override {
    VELOX_FAIL("Unexpected read client request.");
  }

  std::unique_ptr<AzureDataLakeFileClient> getWriteFileClient(
      const std::shared_ptr<AbfsPath>& path,
      const config::ConfigBase&) override {
    Azure::Storage::Files::DataLake::DataLakeClientOptions options;
    options.Transport.Transport = transport_;
    options.Retry.MaxRetries = 0;
    return std::make_unique<DataLakeFileClientWrapper>(
        std::make_unique<Azure::Storage::Files::DataLake::DataLakeFileClient>(
            path->getUrl(false), options));
  }

 private:
  const std::shared_ptr<DeleteTransport> transport_;
};

TEST(AbfsFileSystemRemoveTest, sdkDeleteAndMissingFile) {
  using Azure::Core::Http::HttpStatusCode;
  AbfsFileSystem filesystem(
      std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>{}));
  const std::string path =
      "abfss://container@remove-sdk.dfs.core.windows.net/preview_0.parquet";
  for (const auto& [status, errorCode] :
       {std::pair{HttpStatusCode::Ok, ""},
        std::pair{HttpStatusCode::NotFound, "PathNotFound"},
        std::pair{HttpStatusCode::NotFound, "FilesystemNotFound"},
        std::pair{HttpStatusCode::Forbidden, "AuthorizationPermissionMismatch"},
        std::pair{HttpStatusCode::InternalServerError, "InternalError"}}) {
    auto transport = std::make_shared<DeleteTransport>(status, errorCode);
    AzureClientProviderFactories::registerFactory(
        "remove-sdk", [transport](const std::string&) {
          return std::make_unique<SdkAzureClientProvider>(transport);
        });
    if (status == HttpStatusCode::Forbidden ||
        status == HttpStatusCode::InternalServerError) {
      VELOX_ASSERT_THROW(filesystem.remove(path), "Operation 'Delete'");
    } else {
      EXPECT_NO_THROW(filesystem.remove(path));
    }
    EXPECT_EQ(transport->requests, 1);
  }
}

class DeleteSasTokenProvider final : public SasTokenProvider {
 public:
  std::string getSasToken(
      const std::string& filesystem,
      const std::string& path,
      const std::string& operation) override {
    EXPECT_EQ(filesystem, "container");
    EXPECT_EQ(path, "preview_0.parquet");
    EXPECT_EQ(operation, kAbfsDeleteOperation);
    ++requests;
    throw std::runtime_error("delete token requested");
  }

  int requests{0};
};

TEST(AbfsFileSystemRemoveTest, dynamicSasRequestsDeletePermission) {
  auto tokens = std::make_shared<DeleteSasTokenProvider>();
  DynamicSasTokenClientProvider provider(tokens);
  auto path = std::make_shared<AbfsPath>(
      "abfss://container@remove-token.dfs.core.windows.net/preview_0.parquet");
  auto client = provider.getWriteFileClient(path, config::ConfigBase({}));
  EXPECT_THROW(client->remove(), std::runtime_error);
  EXPECT_THROW(client->remove(), std::runtime_error);
  EXPECT_EQ(tokens->requests, 2);
}

} // namespace
} // namespace facebook::velox::filesystems
