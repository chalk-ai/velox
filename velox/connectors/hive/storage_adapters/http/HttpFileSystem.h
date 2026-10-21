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

#include "velox/connectors/hive/storage_adapters/http/HttpConfig.h"
#include "velox/common/file/FileSystems.h"

namespace facebook::velox::filesystems {

/// Initializes the global curl state required by HttpFileSystem. Safe to call
/// multiple times; initialization happens once.
bool initializeHttp();

/// Releases global curl state. Safe to call without a preceding initialize.
void finalizeHttp();

/// Lightweight read-only filesystem backed by HTTP GET requests.
class HttpFileSystem : public FileSystem {
  const HttpConfig http_config_;

 public:
  explicit HttpFileSystem(std::shared_ptr<const config::ConfigBase> config);

  std::string name() const override;

  std::string_view extractPath(std::string_view path) const override;

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view path,
      const FileOptions& options = {}) override;

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view path,
      const FileOptions& options = {}) override;

  void remove(std::string_view path) override;

  void rename(
      std::string_view source,
      std::string_view destination,
      bool overwrite = false) override;

  bool exists(std::string_view path) override;

  std::vector<std::string> list(std::string_view path) override;

  void mkdir(std::string_view path, const DirectoryOptions& options = {})
      override;

  void rmdir(std::string_view path) override;

};

} // namespace facebook::velox::filesystems
