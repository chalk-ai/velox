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

#include "velox/common/process/StackTrace.h"

#include <algorithm>
#include <fstream>

// Symbolizer requires folly to be compiled with libelf and libdwarf support
// (also currently only works in Linux).
#if __linux__ && FOLLY_HAVE_ELF && FOLLY_HAVE_DWARF
#define VELOX_HAS_SYMBOLIZER 1
#else
#define VELOX_HAS_SYMBOLIZER 0
#endif

#include <fmt/format.h>
#include <folly/Indestructible.h>
#include <folly/String.h>
#include <folly/experimental/symbolizer/StackTrace.h>

#include "velox/common/process/ProcessBase.h"

#include <fmt/format.h>
#include <folly/Indestructible.h>
#include <folly/String.h>
#include <folly/experimental/symbolizer/StackTrace.h>
#include <folly/fibers/FiberManagerInternal.h>

#include "velox/common/process/ProcessBase.h"

namespace facebook::velox::process {

StackTrace::StackTrace(int32_t skipFrames) : skip_frames_(skipFrames) {
  StackTrace::create(skipFrames);
}

StackTrace::StackTrace(const StackTrace& other) {
  skip_frames_ = other.skip_frames_;
  st_ = other.st_;
  if (folly::test_once(other.bt_flag_)) {
    bt_ = other.bt_;
    folly::call_once(bt_flag_, [] {}); // Set the flag.
  }
  if (folly::test_once(other.resolver_flag_)) {
    init_resolver();
    folly::call_once(resolver_flag_, [] {}); // Set the flag.
  }
  if (folly::test_once(other.bt_vector_flag_)) {
    bt_vector_ = other.bt_vector_;
    folly::call_once(bt_vector_flag_, [] {}); // Set the flag.
  }
}

StackTrace& StackTrace::operator=(const StackTrace& other) {
  if (this != &other) {
    this->~StackTrace();
    new (this) StackTrace(other);
  }
  return *this;
}

void StackTrace::create(int32_t skipFrames) {
  const int32_t kMaxFrames = 75;

  auto framecount = st_.load_here(kMaxFrames);
  skipFrames = st_.skip_n_firsts();
}

///////////////////////////////////////////////////////////////////////////////
// reporting functions

const std::vector<void*>& StackTrace::getStack() const {
  return st_.getStackTracePointers();
}

const std::vector<std::string>& StackTrace::toStrVector() const {
  folly::call_once(bt_vector_flag_, [&] {
     size_t frame = 0;
  static folly::Indestructible<folly::fbstring> myname{
      folly::demangle(typeid(decltype(*this))) + "::"};
  bt_vector_.reserve(st_.size());
  for (int i = 0; i < st_.size(); ++i) {
    auto trace = st_[i];
    auto framename = translateFrame(&trace, true);
    if (folly::StringPiece(framename).startsWith(*myname)) {
      continue; // ignore frames in the StackTrace class
    }
    bt_vector_.push_back(fmt::format("# {:<2d} {}", frame++, framename));
  }
  });
  return bt_vector_;
}

const std::string& StackTrace::toString() const {
  folly::call_once(bt_flag_, [&] {
    const auto& vec = toStrVector();
    size_t needed = 0;
    for (const auto& frame : vec) {
      needed += frame.size() + 1;
    }
    bt_.reserve(needed);
    for (const auto& frame_title : vec) {
      bt_ += frame_title;
      bt_ += '\n';
    }
  });
  return bt_;
}

std::string StackTrace::log(
    const char* errorType,
    std::string* out /* = NULL */) const {
  auto pid = folly::to<std::string>(getProcessId());

  std::string msg;
  msg += "Host: " + getHostName();
  msg += "\nProcessID: " + pid;
  msg += "\nThreadID: " +
      folly::to<std::string>(reinterpret_cast<uintptr_t>(getThreadId()));
  msg += "\nName: " + getAppName();
  msg += "\nType: ";
  if (errorType) {
    msg += errorType;
  } else {
    msg += "(unknown error)";
  }
  msg += "\n\n";
  msg += toString();
  msg += "\n";

  std::string tracefn = "/tmp/stacktrace." + pid + ".log";
  std::ofstream f(tracefn.c_str());
  if (f) {
    f << msg;
    f.close();
  }

  if (out) {
    *out = msg;
  }
  return tracefn;
}

void StackTrace::init_resolver() const {
  folly::call_once(resolver_flag_, [&] {
      resolver_.load_stacktrace(st_);
  });
}

std::string StackTrace::translateFrame(void* addressPtr, bool lineNumbers) const {
  init_resolver();
  return folly::fibers::runInMainContext(
      [addressPtr, &resolver = resolver_]() {
        auto* trace = reinterpret_cast<backward_velox::Trace*>(addressPtr);
        backward_velox::ResolvedTrace resolved_trace = resolver.resolve(*trace);
        std::ostringstream stream;
        backward_velox::Printer printer;
        printer.print_trace(resolved_trace, stream);
        return stream.str();
      });
}

std::string StackTrace::demangle(const char* mangled) const {
  std::string demangled;
  folly::call_once(resolver_flag_, [&] {
    demangled = resolver_.demangle(mangled);
  });
  return demangled;
}
}
