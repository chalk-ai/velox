#pragma once

#include <chrono>

#include "velox/common/file/FileSystems.h"

namespace facebook::velox::filesystems {

struct HttpConfig {
  std::optional<std::chrono::milliseconds> connect_timeout = std::nullopt;
  std::optional<std::chrono::milliseconds> request_timeout = std::nullopt;

  HttpConfig(const std::shared_ptr<const config::ConfigBase> &config);
};

}
