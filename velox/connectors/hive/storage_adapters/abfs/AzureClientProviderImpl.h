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

#pragma once

#include "velox/common/config/Config.h"
#include "velox/connectors/hive/storage_adapters/abfs/AbfsPath.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureBlobClient.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureClientProvider.h"
#include "velox/connectors/hive/storage_adapters/abfs/AzureDataLakeFileClient.h"

// AbfsPath.h provides:
//   using namespace Azure::Storage::Blobs;
//   using namespace Azure::Storage::Files::DataLake;

namespace facebook::velox::filesystems {

class DataLakeFileClientWrapper final : public AzureDataLakeFileClient {
 public:
  explicit DataLakeFileClientWrapper(
      std::unique_ptr<DataLakeFileClient> client)
      : client_(std::move(client)) {}

  void create() override;
  Azure::Storage::Files::DataLake::Models::PathProperties getProperties()
      override;
  void append(const uint8_t* buffer, size_t size, uint64_t offset) override;
  void flush(uint64_t position) override;
  void close() override;
  std::string getUrl() override;

 private:
  const std::unique_ptr<DataLakeFileClient> client_;
};

class BlobClientWrapper final : public AzureBlobClient {
 public:
  explicit BlobClientWrapper(std::unique_ptr<BlobClient> client)
      : blobClient_(std::move(client)) {}

  Azure::Response<Azure::Storage::Blobs::Models::BlobProperties> getProperties()
      override;
  Azure::Response<Azure::Storage::Blobs::Models::DownloadBlobResult> download(
      const Azure::Storage::Blobs::DownloadBlobOptions& options) override;
  std::string getUrl() override;

 private:
  std::unique_ptr<BlobClient> blobClient_;
};

// AzureClientProvider for Shared Key authentication.
class SharedKeyAzureClientProvider final : public AzureClientProvider {
 public:
  std::unique_ptr<AzureBlobClient> getReadFileClient(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config) override;

  std::unique_ptr<AzureDataLakeFileClient> getWriteFileClient(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config) override;

  // Test only.
  std::string connectionString(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config);

 private:
  void init(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config);

  std::string connectionString_;
};

// AzureClientProvider for OAuth authentication.
class OAuthAzureClientProvider final : public AzureClientProvider {
 public:
  std::unique_ptr<AzureBlobClient> getReadFileClient(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config) override;

  std::unique_ptr<AzureDataLakeFileClient> getWriteFileClient(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config) override;

  // Test only.
  std::pair<std::string, std::string> tenantIdAndAuthorityHost(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config);

 private:
  void init(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config);

  std::string tenentId_;
  std::string authorityHost_;
  std::shared_ptr<Azure::Core::Credentials::TokenCredential> tokenCredential_;
};

// AzureClientProvider for DefaultAzureCredential authentication.
// Uses the Azure SDK's DefaultAzureCredential which automatically tries
// multiple credential sources (environment variables, managed identity,
// Azure CLI, etc.) in sequence.
class DefaultAzureCredentialProvider final : public AzureClientProvider {
 public:
  std::unique_ptr<AzureBlobClient> getReadFileClient(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config) override;

  std::unique_ptr<AzureDataLakeFileClient> getWriteFileClient(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config) override;
};

// AzureClientProvider for SAS authentication with a fixed SAS token.
class FixedSasAzureClientProvider final : public AzureClientProvider {
 public:
  std::unique_ptr<AzureBlobClient> getReadFileClient(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config) override;

  std::unique_ptr<AzureDataLakeFileClient> getWriteFileClient(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config) override;

  // Test only.
  std::string sas(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config);

 private:
  void init(
      const std::shared_ptr<AbfsPath>& abfsPath,
      const config::ConfigBase& config);

  std::string sas_;
};

} // namespace facebook::velox::filesystems
