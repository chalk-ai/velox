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

namespace facebook::velox::process {

///////////////////////////////////////////////////////////////////////////////

// TODO: Deprecate in favor of folly::symbolizer.
template <typename StackTrace>
class StackTraceImpl {
 public:
  /**
   * Translate a frame pointer to file name and line number pair.
   */
  virtual std::string translateFrame(void* framePtr, bool lineNumbers = true) const = 0;

  /**
   * Demangle a function name.
   */
  virtual std::string demangle(const char* mangled) const = 0;

 public:
  /**
   * Constructor -- saves the current stack trace. By default, we skip the
   * frames for StackTrace::StackTrace.  If you want those, you can pass
   * '-2' to skipFrames.
   */
  explicit StackTraceImpl(int32_t skipFrames = 0) : skip_frames_(skipFrames) {}
  virtual ~StackTraceImpl() {}

  StackTraceImpl(const StackTraceImpl& other) {
    skip_frames_ = other.skip_frames_;
  }

  /**
   * Generate an output of the written stack trace.
   */
  virtual const std::string& toString() const = 0;

  /**
   * Generate a vector that for each position has the title of the frame.
   */
  virtual const std::vector<std::string>& toStrVector() const = 0;

  /**
   * Return the raw stack pointers.
   */
  virtual const std::vector<void*>& getStack() const = 0;

  /**
   * Log stacktrace into a file under /tmp. If "out" is not null,
   * also store translated stack trace into the variable.
   * Returns the name of the generated file.
   */
  virtual std::string log(const char* errorType, std::string* out = nullptr) const = 0;

 protected:
  /**
   * Record bt pointers.
   */
  virtual void create(int32_t skipFrames) = 0;

  int32_t skip_frames_;
};

#ifdef __APPLE__

#include <backward.h>

class AppleStackTrace : public StackTraceImpl<AppleStackTrace> {
  explicit AppleStackTrace(int32_t skipFrames = 0);

  virtual ~AppleStackTrace() override {}

  AppleStackTrace(const AppleStackTrace& other);
  AppleStackTrace& operator=(const AppleStackTrace& other);

  std::string translateFrame(void* framePtr, bool lineNumbers) const override;
  std::string demangle(const char* mangled) const override;
  const std::string& toString() const override;
  const std::vector<std::string>& toStrVector() const override;
  const std::vector<void*>& getStack() const override;
  std::string log(const char* errorType, std::string* out) const override;

protected:
  void create(int32_t skipFrames) override;
  void init_resolver() const;

private:
  backward::StackTrace st_;
  mutable folly::once_flag resolver_flag_;
  mutable backward::TraceResolver resolver_;
  mutable folly::once_flag bt_vector_flag_;
  mutable std::vector<std::string> bt_vector_;
};

using StackTrace = AppleStackTrace;

#else

class LinuxStackTrace : public StackTraceImpl<LinuxStackTrace> {
public:
  explicit AppleStackTrace(int32_t skipFrames = 0);
  virtual ~LinuxStackTrace() override {}

  LinuxStackTrace(const LinuxStackTrace& other);
  LinuxStackTrace& operator=(const LinuxStackTrace& other);

  std::string translateFrame(void* framePtr, bool lineNumbers) const override;
  std::string demangle(const char* mangled) const override;
  const std::string& toString() const override;
  const std::vector<std::string>& toStrVector() const override;
  const std::vector<void*>& getStack() const override;
  std::string log(const char* errorType, std::string* out) const override;

protected:
  void create(int32_t skipFrames) override;

private:
  mutable folly::once_flag bt_vector_flag_;
  mutable std::vector<std::string> bt_vector_;
  mutable folly::once_flag bt_flag_;
  mutable std::string bt_;
  std::vector<void*> bt_pointers_;
};

using StackTrace = LinuxStackTrace;

#endif

///////////////////////////////////////////////////////////////////////////////
} // namespace facebook::velox::process
