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

#include "velox/common/memory/MallocAllocator.h"

#include <barrier>
#include <cstring>
#include <latch>
#include <memory>
#include <thread>
#include <vector>

#include <folly/system/HardwareConcurrency.h>
#include <gtest/gtest.h>

namespace facebook::velox::memory::test {
namespace {

struct FreeAllocation {
  MallocAllocator* allocator;
  size_t bytes;

  void operator()(char* allocation) const {
    allocator->freeBytes(allocation, bytes);
  }
};

using OwnedAllocation = std::unique_ptr<char, FreeAllocation>;

OwnedAllocation allocate(MallocAllocator& allocator, size_t bytes) {
  return OwnedAllocation(
      static_cast<char*>(allocator.allocateBytes(bytes)),
      FreeAllocation{&allocator, bytes});
}

TEST(MallocAllocatorReservationTest, crossThreadFreesAndThreadChurn) {
  constexpr int kNumThreads = 16;
  constexpr size_t kReservationBytes = 4096;
  MemoryAllocator::Options options;
  options.reservationByteLimit = kReservationBytes;
  options.capacity = 64 << 20;
  MallocAllocator allocator(options);
  for (int cycle = 0; cycle < 8; ++cycle) {
    std::vector<std::vector<OwnedAllocation>> allocations(kNumThreads);
    std::vector<std::thread> threads;
    for (int thread = 0; thread < kNumThreads; ++thread) {
      threads.emplace_back([&, thread] {
        for (const size_t bytes : {64, 4095, 4096, 4097, 16384}) {
          for (int allocation = 0; allocation < 8; ++allocation) {
            auto buffer = allocate(allocator, bytes);
            ASSERT_NE(buffer, nullptr);
            std::memset(buffer.get(), thread, bytes);
            allocations[thread].push_back(std::move(buffer));
          }
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    constexpr size_t kLiveBytes =
        kNumThreads * 8 * (64 + 4095 + 4096 + 4097 + 16384);
    ASSERT_EQ(allocator.totalUsedBytes(), kLiveBytes);
    ASSERT_TRUE(allocator.checkConsistency());

    // All allocations outlive their allocating threads and are freed elsewhere.
    threads.clear();
    for (int thread = 0; thread < kNumThreads; ++thread) {
      threads.emplace_back([&, thread] {
        auto& buffers = allocations[(thread + 1) % kNumThreads];
        for (auto& buffer : buffers) {
          EXPECT_EQ(buffer.get()[0], (thread + 1) % kNumThreads);
          EXPECT_EQ(
              buffer.get()[buffer.get_deleter().bytes - 1],
              (thread + 1) % kNumThreads);
          buffer.reset();
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
    ASSERT_EQ(allocator.totalUsedBytes(), 0);
    ASSERT_TRUE(allocator.checkConsistency());
  }
}

TEST(MallocAllocatorReservationTest, concurrentCapRejectionAndRecovery) {
  constexpr int kNumThreads = 16;
  constexpr size_t kReservationBytes = 4096;
  MemoryAllocator::Options options;
  options.reservationByteLimit = kReservationBytes;
  // Leave room for every shard's retained credits as well as live allocations.
  const auto numShards = bits::nextPowerOfTwo(folly::available_concurrency());
  options.capacity = (2 * numShards + kNumThreads) * kReservationBytes;
  for (int cycle = 0; cycle < 8; ++cycle) {
    MallocAllocator allocator(options);
    // Fill the cap using the unreserved path, then reject simultaneous refills.
    auto blocker = allocate(allocator, options.capacity);
    ASSERT_NE(blocker, nullptr);
    std::barrier start(kNumThreads);
    std::latch rejected(kNumThreads);
    std::latch resume(1);
    std::latch allocated(kNumThreads);
    std::latch release(1);
    std::vector<OwnedAllocation> allocations;
    for (int thread = 0; thread < kNumThreads; ++thread) {
      allocations.emplace_back(nullptr, FreeAllocation{&allocator, 64});
    }
    std::vector<std::thread> threads;
    for (int thread = 0; thread < kNumThreads; ++thread) {
      threads.emplace_back([&, thread] {
        start.arrive_and_wait();
        for (int attempt = 0; attempt < 16; ++attempt) {
          EXPECT_EQ(allocate(allocator, 64), nullptr);
          EXPECT_EQ(allocate(allocator, kReservationBytes), nullptr);
        }
        rejected.count_down();
        resume.wait();
        allocations[thread] = allocate(allocator, 64);
        EXPECT_NE(allocations[thread], nullptr);
        allocated.count_down();
        release.wait();
        // The release latch publishes all handles before ownership rotates.
        allocations[(thread + 1) % kNumThreads].reset();
      });
    }
    rejected.wait();
    EXPECT_EQ(allocator.totalUsedBytes(), options.capacity);
    EXPECT_TRUE(allocator.checkConsistency());
    blocker.reset();
    resume.count_down();
    allocated.wait();
    EXPECT_EQ(allocator.totalUsedBytes(), kNumThreads * 64);
    EXPECT_TRUE(allocator.checkConsistency());
    release.count_down();
    for (auto& thread : threads) {
      thread.join();
    }
    EXPECT_EQ(allocator.totalUsedBytes(), 0);
    EXPECT_TRUE(allocator.checkConsistency());
  }
}

TEST(MallocAllocatorReservationTest, exactReservationBoundaryAndRollback) {
  MemoryAllocator::Options options;
  options.reservationByteLimit = 4096;
  options.capacity = 8192;
  MallocAllocator allocator(options);
  for (int cycle = 0; cycle < 32; ++cycle) {
    auto large = allocate(allocator, 4096);
    ASSERT_NE(large, nullptr);
    auto small = allocate(allocator, 4095);
    ASSERT_NE(small, nullptr);
    ASSERT_EQ(allocator.totalUsedBytes(), 8191);
    // Exactly consuming the last reserved byte requires a refill, which fails.
    EXPECT_EQ(allocate(allocator, 1), nullptr);
    EXPECT_EQ(allocate(allocator, 4096), nullptr);
    EXPECT_EQ(allocator.totalUsedBytes(), 8191);
    small.reset();
    large.reset();
    ASSERT_EQ(allocator.totalUsedBytes(), 0);
    ASSERT_TRUE(allocator.checkConsistency());
  }
}

} // namespace
} // namespace facebook::velox::memory::test
