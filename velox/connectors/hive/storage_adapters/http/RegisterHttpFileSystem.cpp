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
constexpr std::string_view kHttpScheme{"http://"};
constexpr std::string_view kHttpsScheme{"https://"};

bool isHttpFile(std::string_view path) {
  return path.starts_with(kHttpScheme) || path.starts_with(kHttpsScheme);
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
  fileSystems().withWLock([&](auto& instanceMap) {
    bool single_use = std::all_of(instanceMap.begin(), instanceMap.end(), [](const auto& kv) {
      return kv.second.use_count() == 1;
    });
    VELOX_CHECK(singleUseCount, "Cannot finalize HttpFileSystem while in use");
    instanceMap.clear();
  });

  finalizeHttp();
#endif
}
} // namespace facebook::velox::filesystems
