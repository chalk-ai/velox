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

#include <chrono>

#include "velox/common/file/FileSystems.h"

namespace facebook::velox::filesystems {

struct HttpConfig {
  std::optional<std::chrono::milliseconds> connect_timeout = std::nullopt;
  std::optional<std::chrono::milliseconds> request_timeout = std::nullopt;

  HttpConfig(const std::shared_ptr<const config::ConfigBase> &config);
};

}
