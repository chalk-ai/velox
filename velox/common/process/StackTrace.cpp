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

// #include "velox/common/process/StackTrace.h"

// #include <stacktrace>

// #include <fmt/format.h>
// #include <folly/Indestructible.h>
// #include <folly/String.h>
// #include <folly/experimental/symbolizer/StackTrace.h>
// #include <string>

// #include "velox/common/process/ProcessBase.h"

// namespace facebook::velox::process {

// StackTrace::StackTrace(int32_t skipFrames) {
//   (void)skipFrames;
//   auto stack = std::stacktrace::current();
//   // TODO: respect skipFrames
//   size_t index = 0;
//   for (const auto& func_row : stack) {
//     std::string func_row_name = func_row.to_string();
//     this->_stack_frame_formatted += "@stack ";
//     index += 1;
//     this->_stack_frame_formatted += std::to_string(index);
//     this->_stack_frame_formatted += func_row_name;
//     this->_stack_frame_formatted += "\n";
//     stack_frame_list.push_back(std::move(func_row_name));
//   }
// }

// ///////////////////////////////////////////////////////////////////////////////
// // reporting functions

// } // namespace facebook::velox::process
