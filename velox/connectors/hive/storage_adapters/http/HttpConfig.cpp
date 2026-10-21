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
#include "velox/connectors/hive/storage_adapters/http/HttpConfig.h"

#include "common/config/Config.h"

namespace {
constexpr std::string_view kCurlRequestTimeoutMsConfigKey =
    "curl.request.timeout.ms";
constexpr std::string_view kCurlConnectTimeoutMsConfigKey =
    "curl.connect.timeout.ms";
} // namespace

facebook::velox::filesystems::HttpConfig::HttpConfig(
    const std::shared_ptr<const config::ConfigBase>& config) {
  if (const auto conn_timeout_ms =
          config->get<int64_t>(std::string(kCurlConnectTimeoutMsConfigKey));
      conn_timeout_ms.has_value() && conn_timeout_ms.value() > 0) {
    connect_timeout = std::chrono::milliseconds(conn_timeout_ms.value());
  }

  if (const auto request_timeout_ms =
          config->get<int64_t>(std::string(kCurlRequestTimeoutMsConfigKey));
      request_timeout_ms.has_value() && request_timeout_ms.value() > 0) {
    request_timeout = std::chrono::milliseconds(request_timeout_ms.value());
  }
}
