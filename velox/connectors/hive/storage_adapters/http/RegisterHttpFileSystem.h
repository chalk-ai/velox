#pragma once

#include <functional>
#include <memory>
#include <string>

namespace facebook::velox::config {
class ConfigBase;
}

namespace facebook::velox::filesystems {

using CacheKeyFn = std::function<
    std::string(std::shared_ptr<const config::ConfigBase>, std::string_view)>;

void registerHttpFileSystem(CacheKeyFn cacheKeyFunc = nullptr);

void finalizeHttpFileSystem();

} // namespace facebook::velox::filesystems
