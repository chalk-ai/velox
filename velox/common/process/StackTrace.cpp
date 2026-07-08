/// NOTE: This file is rewritten in the Chalk fork to use 'backward'
/// for getting stack traces, instead of Folly.

#include <string>

#include <backward.hpp>
#include <fmt/format.h>
#include <folly/Indestructible.h>
#include <folly/String.h>
#include <folly/debugging/symbolizer/StackTrace.h>

#include "velox/common/process/ProcessBase.h"
#include "velox/common/process/StackTrace.h"

#ifdef __linux__
#include <folly/debugging/symbolizer/Symbolizer.h> // @manual
#include <folly/fibers/FiberManager.h> // @manual
#endif

namespace facebook::velox::process {

StackTrace::StackTrace(int32_t skipFrames) {
  (void)skipFrames;
  backward::StackTrace backtrace;
  backtrace.load_here();

  backward::TraceResolver backtrace_resolver;
  backtrace_resolver.load_stacktrace(backtrace);

  for (size_t i = 0; i < backtrace.size(); ++i) {
    backward::ResolvedTrace trace = backtrace_resolver.resolve(backtrace[i]);
    std::string stack_line;
    if (trace.source.filename != "") {
      stack_line = fmt::format(
          "@{}: {}:{} {}",
          i,
          trace.source.filename,
          trace.source.line,
          trace.object_function);
    } else {
      stack_line = fmt::format("@{}: {}", i, trace.object_function);
    }

    this->_stack_frame_formatted += stack_line;
    this->_stack_frame_formatted += "\n";
    this->_stack_frame_list.push_back(std::move(stack_line));
  }
}

} // namespace facebook::velox::process
