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

#include "velox/connectors/hive/storage_adapters/http/RegisterHttpFileSystem.h" // @manual

#ifdef VELOX_ENABLE_HTTP
#include "velox/connectors/hive/storage_adapters/http/HttpFileSystem.h"
#include "velox/common/file/FileSystems.h"
#include <folly/Synchronized.h>
#include <unordered_map>
#endif

namespace facebook::velox::filesystems {

#ifdef VELOX_ENABLE_HTTP
using FileSystemMap = folly::Synchronized<
    std::unordered_map<std::string, std::shared_ptr<FileSystem>>>;

FileSystemMap& fileSystems() {
  static FileSystemMap instances;
  return instances;
}

CacheKeyFn cacheKeyFunc;

namespace {

bool isHttpFile(std::string_view path) {
  return path.rfind("http://", 0) == 0 || path.rfind("https://", 0) == 0;
}

std::string defaultCacheKey(
    const std::shared_ptr<const config::ConfigBase>& /*properties*/,
    std::string_view httpPath) {
  auto schemePos = httpPath.find("://");
  if (schemePos == std::string_view::npos) {
    return "http-default";
  }
  auto authorityStart = schemePos + 3;
  auto authorityEnd = httpPath.find('/', authorityStart);
  if (authorityEnd == std::string_view::npos) {
    authorityEnd = httpPath.size();
  }
  return std::string(httpPath.substr(0, authorityEnd));
}

} // namespace

std::shared_ptr<FileSystem> fileSystemGenerator(
    std::shared_ptr<const config::ConfigBase> properties,
    std::string_view httpPath) {
  auto keyFn = cacheKeyFunc ? cacheKeyFunc : defaultCacheKey;
  auto cacheKey = keyFn(properties, httpPath);

  // Check if an instance exists with a read lock (shared).
  auto fs = fileSystems().withRLock(
      [&](auto& instanceMap) -> std::shared_ptr<FileSystem> {
        auto iterator = instanceMap.find(cacheKey);
        if (iterator != instanceMap.end()) {
          return iterator->second;
        }
        return nullptr;
      });
  if (fs != nullptr) {
    return fs;
  }

  return fileSystems().withWLock(
      [&](auto& instanceMap) -> std::shared_ptr<FileSystem> {
        // Repeat the checks with a write lock.
        auto iterator = instanceMap.find(cacheKey);
        if (iterator != instanceMap.end()) {
          return iterator->second;
        }

        initializeHttp();
        auto fs = std::make_shared<HttpFileSystem>(properties);
        instanceMap.insert({cacheKey, fs});
        return fs;
      });
}
#endif

void registerHttpFileSystem(CacheKeyFn identityFunction) {
#ifdef VELOX_ENABLE_HTTP
  fileSystems().withWLock([&](auto& instanceMap) {
    if (instanceMap.empty()) {
      cacheKeyFunc = identityFunction;
      registerFileSystem(isHttpFile, std::function(fileSystemGenerator));
    }
  });
#endif
}

void finalizeHttpFileSystem() {
#ifdef VELOX_ENABLE_HTTP
  bool singleUseCount = true;
  fileSystems().withWLock([&](auto& instanceMap) {
    for (const auto& [id, fs] : instanceMap) {
      singleUseCount &= (fs.use_count() == 1);
    }
    VELOX_CHECK(singleUseCount, "Cannot finalize HttpFileSystem while in use");
    instanceMap.clear();
  });

  finalizeHttp();
#endif
}

void registerHttpMetrics() {
#ifdef VELOX_ENABLE_HTTP
  // No HTTP-specific metrics are registered yet.
#endif
}

} // namespace facebook::velox::filesystems
