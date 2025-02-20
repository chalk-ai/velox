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

#include <folly/synchronization/CallOnce.h>

#include <string>
#include <vector>
#include "backward-cpp/backward_velox.h"

namespace facebook::velox::process {

///////////////////////////////////////////////////////////////////////////////

// TODO: Deprecate in favor of folly::symbolizer.
class StackTrace {
 public:
  /**
   * Translate a frame pointer to file name and line number pair.
   */
  std::string translateFrame(void* framePtr, bool lineNumbers = true) const;

  /**
   * Demangle a function name.
   */
  std::string demangle(const char* mangled) const;

 public:
  /**
   * Constructor -- saves the current stack trace. By default, we skip the
   * frames for StackTrace::StackTrace.  If you want those, you can pass
   * '-2' to skipFrames.
   */
  explicit StackTrace(int32_t skipFrames = 0);
  ~StackTrace() {}

  StackTrace(const StackTrace& other);
  StackTrace& operator=(const StackTrace& other);

  /**
   * Generate an output of the written stack trace.
   */
  const std::string& toString() const;

  /**
   * Generate a vector that for each position has the title of the frame.
   */
  const std::vector<std::string>& toStrVector() const;

  /**
   * Return the raw stack pointers.
   */
  const std::vector<void*>& getStack() const;

  /**
   * Log stacktrace into a file under /tmp. If "out" is not null,
   * also store translated stack trace into the variable.
   * Returns the name of the generated file.
   */
  std::string log(const char* errorType, std::string* out = nullptr) const;

 protected:
  /**
   * Record bt pointers.
   */
  void create(int32_t skipFrames);
  void init_resolver() const;

private:
  mutable backward_velox::StackTrace st_;
  mutable std::string bt_;
  mutable folly::once_flag bt_flag_;
  mutable folly::once_flag resolver_flag_;
  mutable backward_velox::TraceResolver resolver_;
  mutable folly::once_flag bt_vector_flag_;
  mutable std::vector<std::string> bt_vector_;
  int32_t skip_frames_;
};

///////////////////////////////////////////////////////////////////////////////
} // namespace facebook::velox::process
