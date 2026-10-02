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

#include <algorithm>
#include <atomic>
#include <barrier>
#include <limits>
#include <stdexcept>

#include <fmt/format.h>
#include <folly/Random.h>
#include <folly/system/HardwareConcurrency.h>
#include <gtest/gtest.h>
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox::common::test {

class ConcurrentCounterTest : public testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    setupCounter();
  }

  void update(int64_t delta) {
    if (useUpdateFn_) {
      counter_->update(
          delta, [&](int64_t& counter, int64_t delta, std::mutex& lock) {
            std::lock_guard<std::mutex> l(lock);
            counter += delta;
            return true;
          });
    } else {
      counter_->update(delta);
    }
  }

  int64_t read() const {
    return counter_->read();
  }

  void setupCounter() {
    counter_ = std::make_unique<ConcurrentCounter<int64_t>>(
        folly::available_concurrency());
  }

  const bool useUpdateFn_{GetParam()};

  std::unique_ptr<ConcurrentCounter<int64_t>> counter_;
};

TEST_P(ConcurrentCounterTest, basic) {
  ASSERT_EQ(read(), 0);
  update(1);
  ASSERT_EQ(read(), 1);
  update(1);
  ASSERT_EQ(read(), 2);
  update(-1);
  ASSERT_EQ(read(), 1);
  update(-3);
  ASSERT_EQ(read(), -2);
}

TEST_P(ConcurrentCounterTest, alignedThreadHashes) {
  // Model aligned pthread IDs independently of the host's standard library.
  for (const size_t requestedShards : {3, 16, 127, 256}) {
    const auto numShards = bits::nextPowerOfTwo(requestedShards);
    ConcurrentCounter<int64_t> counter(requestedShards);
    for (const size_t offset : {0, 0x6c0}) {
      for (const size_t stride : {256, 4096, 1 << 20}) {
        SCOPED_TRACE(
            fmt::format(
                "shards: {}, offset: {}, stride: {}",
                numShards,
                offset,
                stride));
        std::vector<size_t> occupancy(numShards, 0);
        for (size_t thread = 0; thread < numShards * 32; ++thread) {
          const size_t hash = offset + thread * stride;
          const auto shard = counter.testingShardIndex(hash);
          ASSERT_LT(shard, numShards);
          ASSERT_EQ(counter.testingShardIndex(hash), shard);
          ++occupancy[shard];
        }
        // Generous bounds detect collapsed low bits without prescribing a hash.
        EXPECT_GE(
            std::count_if(
                occupancy.begin(),
                occupancy.end(),
                [](auto count) { return count != 0; }),
            numShards * 3 / 4);
        EXPECT_LT(*std::max_element(occupancy.begin(), occupancy.end()), 128);
        EXPECT_LT(
            counter.testingShardIndex(std::numeric_limits<size_t>::max()),
            numShards);
      }
    }
  }
}

TEST_P(ConcurrentCounterTest, forcedCollisionsAndConcurrentReads) {
  constexpr int kNumThreads = 16;
  constexpr int kNumUpdates = 2'000;
  for (const size_t shards : {1, 3, 64}) {
    SCOPED_TRACE(fmt::format("shards: {}", shards));
    counter_ = std::make_unique<ConcurrentCounter<int64_t>>(shards);
    std::barrier start(kNumThreads + 1);
    std::atomic<int> remaining{kNumThreads};
    std::vector<std::thread> threads;
    for (int thread = 0; thread < kNumThreads; ++thread) {
      threads.emplace_back([&] {
        start.arrive_and_wait();
        for (int iteration = 0; iteration < kNumUpdates; ++iteration) {
          update(1);
        }
        remaining.fetch_sub(1);
      });
    }
    start.arrive_and_wait();
    int64_t previous = 0;
    while (remaining.load() != 0) {
      const auto current = read();
      EXPECT_GE(current, previous);
      EXPECT_LE(current, kNumThreads * kNumUpdates);
      previous = current;
      std::this_thread::yield();
    }
    for (auto& thread : threads) {
      thread.join();
    }
    EXPECT_EQ(read(), kNumThreads * kNumUpdates);
    update(-kNumThreads * kNumUpdates);
    EXPECT_EQ(read(), 0);
  }
}

TEST_P(ConcurrentCounterTest, rejectedAndThrowingUpdates) {
  ConcurrentCounter<int64_t> counter(1);
  constexpr int kNumThreads = 16;
  constexpr int64_t kCapacity = 257;
  std::barrier start(kNumThreads);
  std::atomic<int64_t> successes{0};
  std::vector<std::thread> threads;
  for (int thread = 0; thread < kNumThreads; ++thread) {
    threads.emplace_back([&] {
      start.arrive_and_wait();
      for (int attempt = 0; attempt < 100; ++attempt) {
        const bool accepted = counter.update(
            1, [](int64_t& value, int64_t delta, std::mutex& mutex) {
              std::lock_guard<std::mutex> lock(mutex);
              if (value + delta > kCapacity) {
                return false;
              }
              value += delta;
              return true;
            });
        successes.fetch_add(accepted);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(successes.load(), kCapacity);
  EXPECT_EQ(counter.read(), kCapacity);
  EXPECT_THROW(
      counter.update(
          1,
          [](int64_t&, int64_t, std::mutex& mutex) -> bool {
            std::lock_guard<std::mutex> lock(mutex);
            throw std::runtime_error("reject before modifying the reservation");
          }),
      std::runtime_error);
  EXPECT_EQ(counter.read(), kCapacity);
  counter.update(-kCapacity);
  EXPECT_EQ(counter.read(), 0);
}

TEST_P(ConcurrentCounterTest, multithread) {
  const int32_t numUpdatesPerThread = 5'000;
  std::vector<int> numThreads;
  numThreads.push_back(1);
  numThreads.push_back(folly::available_concurrency());
  numThreads.push_back(folly::available_concurrency() * 2);
  for (int numThreads : numThreads) {
    SCOPED_TRACE(fmt::format("numThreads: {}", numThreads));
    counter_->testingClear();
    ASSERT_EQ(counter_->read(), 0);

    std::vector<std::thread> threads;
    threads.reserve(numThreads);
    std::vector<int64_t> counts(numThreads, 0);
    for (size_t i = 0; i < numThreads; ++i) {
      ASSERT_EQ(counts[i], 0);
      threads.emplace_back([&, i]() {
        folly::Random::DefaultGenerator rng;
        rng.seed(1234 + i);
        ASSERT_EQ(counts[i], 0);
        for (int j = 0; j < numUpdatesPerThread; ++j) {
          const int delta = folly::Random::rand32(rng);
          counts[i] += delta;
          update(delta);
        }
      });
    }

    for (auto& th : threads) {
      th.join();
    }
    int64_t expectedCount{0};
    for (int i = 0; i < numThreads; ++i) {
      expectedCount += counts[i];
    }
    ASSERT_EQ(read(), expectedCount);
  }
}

VELOX_INSTANTIATE_TEST_SUITE_P(
    ConcurrentCounterTestSuite,
    ConcurrentCounterTest,
    testing::ValuesIn({false, true}));

} // namespace facebook::velox::common::test
