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

#include "velox/common/base/ConcurrentCounter.h"
#include "velox/common/memory/MallocAllocator.h"

#include <algorithm>
#include <array>
#include <barrier>
#include <chrono>
#include <exception>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include <folly/system/HardwareConcurrency.h>
#include <gflags/gflags.h>

DEFINE_uint32(num_threads, 32, "Number of concurrent workers");
DEFINE_uint32(
    iterations,
    100000,
    "Updates or allocation/free pairs per worker");
DEFINE_uint32(num_runs, 5, "Measured repetitions after per-thread warmup");
DEFINE_string(
    workload,
    "all",
    "all, counter, callback, malloc64, malloc4096, batch4096, or direct");

namespace facebook::velox {
namespace {

constexpr uint32_t kWarmupIterations = 1000;

// Thread creation, joining, and warmup are outside the measured interval.
// Both barriers include the coordinator so no worker can start timing early.
template <typename Prepare, typename Operation>
double measure(uint32_t iterations, Prepare prepare, Operation operation) {
  using Clock = std::chrono::steady_clock;
  Clock::time_point begin;
  Clock::time_point end;
  std::barrier start(
      FLAGS_num_threads + 1, [&]() noexcept { begin = Clock::now(); });
  std::barrier finish(
      FLAGS_num_threads + 1, [&]() noexcept { end = Clock::now(); });
  std::vector<std::exception_ptr> errors(FLAGS_num_threads);
  std::vector<std::thread> threads;
  for (uint32_t thread = 0; thread < FLAGS_num_threads; ++thread) {
    threads.emplace_back([&, thread] {
      try {
        prepare(thread);
        for (uint32_t iteration = 0; iteration < kWarmupIterations;
             ++iteration) {
          operation();
        }
      } catch (...) {
        errors[thread] = std::current_exception();
      }
      start.arrive_and_wait();
      try {
        if (!errors[thread]) {
          for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
            operation();
          }
        }
      } catch (...) {
        errors[thread] = std::current_exception();
      }
      finish.arrive_and_wait();
    });
  }
  start.arrive_and_wait();
  finish.arrive_and_wait();
  for (auto& thread : threads) {
    thread.join();
  }
  for (const auto& error : errors) {
    if (error) {
      std::rethrow_exception(error);
    }
  }
  return std::chrono::duration<double, std::nano>(end - begin).count();
}

template <typename Sample>
void report(const std::string& name, Sample sample) {
  if (FLAGS_workload != "all" && FLAGS_workload != name) {
    return;
  }
  std::vector<double> samples;
  for (uint32_t run = 0; run < FLAGS_num_runs; ++run) {
    const auto nanoseconds = sample();
    samples.push_back(nanoseconds);
    std::cout << name << ',' << FLAGS_num_threads << ',' << run << ','
              << nanoseconds << '\n';
  }
  std::sort(samples.begin(), samples.end());
  const auto middle = samples.size() / 2;
  const auto median = samples.size() % 2
      ? samples[middle]
      : (samples[middle - 1] + samples[middle]) / 2;
  std::cout << name << ',' << FLAGS_num_threads << ",median," << median << '\n';
}

void counterBenchmark(bool callback) {
  report(callback ? "callback" : "counter", [&] {
    ConcurrentCounter<int64_t> counter(folly::available_concurrency());
    std::vector<size_t> shards(FLAGS_num_threads);
    const ConcurrentCounter<int64_t>::UpdateFn update =
        [](int64_t& value, int64_t delta, std::mutex& mutex) {
          std::lock_guard<std::mutex> lock(mutex);
          value += delta;
          return true;
        };
    const auto elapsed = measure(
        FLAGS_iterations,
        [&](uint32_t thread) {
          shards[thread] = counter.testingShardIndex(
              std::hash<std::thread::id>{}(std::this_thread::get_id()));
        },
        [&] {
          if (callback) {
            counter.update(1, update);
          } else {
            counter.update(1);
          }
        });
    VELOX_CHECK_EQ(
        counter.read(),
        uint64_t{FLAGS_num_threads} * (FLAGS_iterations + kWarmupIterations));
    std::sort(shards.begin(), shards.end());
    std::cerr << "occupied_shards="
              << std::unique(shards.begin(), shards.end()) - shards.begin()
              << '\n';
    return elapsed / (uint64_t{FLAGS_num_threads} * FLAGS_iterations);
  });
}

struct FreeAllocation {
  memory::MallocAllocator* allocator{nullptr};
  size_t bytes{0};

  void operator()(char* allocation) const {
    allocator->freeBytes(allocation, bytes);
  }
};

template <size_t BatchSize>
void allocatorBenchmark(const std::string& name, size_t bytes) {
  report(name, [&] {
    memory::MemoryAllocator::Options options;
    // Fix the shared atomics' cache-line placement across comparison builds.
    alignas(folly::hardware_destructive_interference_size)
        memory::MallocAllocator allocator(options);
    const auto iterations = std::max<uint32_t>(1, FLAGS_iterations / BatchSize);
    const auto elapsed = measure(
        iterations,
        [](uint32_t) {},
        [&] {
          std::array<std::unique_ptr<char, FreeAllocation>, BatchSize> buffers;
          for (auto& buffer : buffers) {
            buffer = std::unique_ptr<char, FreeAllocation>(
                static_cast<char*>(allocator.allocateBytes(bytes)),
                FreeAllocation{&allocator, bytes});
            VELOX_CHECK_NOT_NULL(buffer);
            buffer.get()[0] = 1;
            buffer.get()[bytes - 1] = 2;
          }
        });
    VELOX_CHECK_EQ(allocator.totalUsedBytes(), 0);
    VELOX_CHECK(allocator.checkConsistency());
    return elapsed / (uint64_t{FLAGS_num_threads} * iterations * BatchSize);
  });
}

} // namespace
} // namespace facebook::velox

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  VELOX_CHECK_GT(FLAGS_num_threads, 0);
  VELOX_CHECK_GT(FLAGS_iterations, 0);
  VELOX_CHECK_GT(FLAGS_num_runs, 0);
  const std::vector<std::string> workloads{
      "all",
      "counter",
      "callback",
      "malloc64",
      "malloc4096",
      "batch4096",
      "direct"};
  VELOX_CHECK(
      std::find(workloads.begin(), workloads.end(), FLAGS_workload) !=
      workloads.end());
  std::cerr << "available_concurrency=" << folly::available_concurrency()
            << '\n';
  std::cout << "workload,threads,run,ns_per_update_or_allocation_free_pair\n";
  facebook::velox::counterBenchmark(false);
  facebook::velox::counterBenchmark(true);
  facebook::velox::allocatorBenchmark<1>("malloc64", 64);
  facebook::velox::allocatorBenchmark<1>("malloc4096", 4096);
  facebook::velox::allocatorBenchmark<32>("batch4096", 4096);
  // Requests at the default reservation threshold bypass ConcurrentCounter.
  facebook::velox::allocatorBenchmark<1>("direct", 1 << 20);
}
