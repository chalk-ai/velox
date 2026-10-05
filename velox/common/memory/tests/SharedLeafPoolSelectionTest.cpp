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

#include "velox/common/memory/Memory.h"

#include <latch>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

DECLARE_int32(velox_memory_num_shared_leaf_pools);

namespace facebook::velox::memory::test {
namespace {

TEST(SharedLeafPoolSelectionTest, spreadsAcrossThreads) {
  MemoryManager manager{};
  constexpr int kNumThreads = 64;
  std::mutex mu;
  std::unordered_set<MemoryPool*> pools;
  // Every thread stays alive until all have picked a pool, so no pthread ID is
  // reused within the sample.
  std::latch allPicked(kNumThreads);
  std::vector<std::thread> threads;
  threads.reserve(kNumThreads);
  for (int i = 0; i < kNumThreads; ++i) {
    threads.emplace_back([&]() {
      auto* pool = &manager.deprecatedSharedLeafPool();
      {
        std::lock_guard<std::mutex> l(mu);
        pools.insert(pool);
      }
      allPicked.arrive_and_wait();
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  // Under libc++, an unmixed thread-id hash modulo the power-of-two pool count
  // sends every thread to one pool.
  ASSERT_GE(
      pools.size(),
      static_cast<size_t>(FLAGS_velox_memory_num_shared_leaf_pools / 4));
}

} // namespace
} // namespace facebook::velox::memory::test
