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

#include <string>
#include <vector>

namespace facebook::velox::process {

///////////////////////////////////////////////////////////////////////////////

/// Build a stack-trace.
class StackTrace {
  std::vector<std::string> _stack_frame_list;
  std::string _stack_frame_formatted;

 public:
  explicit StackTrace(int32_t skipFrames = 0);

  StackTrace(const StackTrace& other) = default;
  StackTrace& operator=(const StackTrace& other) = default;

  /// Generate an output of the written stack trace.
  const std::string& toString() const {
    return this->_stack_frame_formatted;
  }

  /// Generate a vector that for each position has the title of the frame.
  const std::vector<std::string>& toStrVector() const {
    return this->_stack_frame_list;
  }
};

///////////////////////////////////////////////////////////////////////////////
} // namespace facebook::velox::process
