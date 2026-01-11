# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

velox_add_library(
  velox_process
  ProcessBase.cpp
  StackTrace.cpp
  ThreadDebugInfo.cpp
  TraceContext.cpp
  TraceHistory.cpp
)

velox_link_libraries(
  velox_process
  PUBLIC velox_file velox_flag_definitions Folly::folly
  PRIVATE fmt::fmt gflags::gflags
)

# BEGIN CHALK MODIFICATION
# Fetch backward-cpp for stack trace support
FetchContent_Declare(
  backward
  GIT_REPOSITORY https://github.com/bombela/backward-cpp
  GIT_TAG 0bfd0a07a61551413ccd2ab9a9099af3bad40681
  EXCLUDE_FROM_ALL)
FetchContent_MakeAvailable(backward)

# Link against Backward::Interface to get proper BACKWARD_HAS_* defines
# and library dependencies (e.g., -lbfd -ldl)
# This ensures backward-cpp uses the correct resolver (libbfd/libdw/libdwarf)
# instead of falling back to the broken backtrace_symbol resolver
velox_link_libraries(
  velox_process
  PRIVATE Backward::Interface
)
# END CHALK MODIFICATION

# Profiler need not be part of the core Velox library
add_library(velox_profiler OBJECT Profiler.cpp)
target_link_libraries(
  velox_profiler
  PUBLIC velox_flag_definitions Folly::folly
  PRIVATE fmt::fmt gflags::gflags glog::glog
)

if(${VELOX_BUILD_TESTING})
  add_subdirectory(tests)
endif()
