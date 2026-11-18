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

#include <cstring>
#include <string>
#include <string_view>

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/MaterializedOutputBuffer.h"

namespace facebook::velox::exec {
namespace {

// Retains a flushed payload independently of its producer.
class RetainingExchangeSink : public ExchangeSink {
 public:
  void append(int32_t /*partition*/, std::string_view /*data*/) override {
    FAIL() << "Expected the IOBuf append overload";
  }

  void append(int32_t /*partition*/, std::unique_ptr<folly::IOBuf> data)
      override {
    data_ = std::move(data);
  }

  CommittedExchangeOutput finish() override {
    return {};
  }

  void abort() override {}

  folly::F14FastMap<std::string, int64_t> stats() const override {
    return {};
  }

  // Keeps the payload alive after the output buffer is destroyed.
  std::unique_ptr<folly::IOBuf> data_;
};

TEST(MaterializedOutputBufferAllocationTest, allocationWithoutPool) {
  auto sink = std::make_shared<RetainingExchangeSink>();
  MaterializedOutputBuffer buffer(1, sink, 1 << 20);
  for (const size_t size : {0, 1'001}) {
    SCOPED_TRACE(size);
    auto data = buffer.allocateTrackedIOBuf(size);
    EXPECT_EQ(data->length(), 0);
    EXPECT_GE(data->capacity(), size);
    if (size > 0) {
      std::memset(data->writableData(), 'x', size);
      data->append(size);
      EXPECT_EQ(data->length(), size);
    }
  }
}

TEST(MaterializedOutputBufferAllocationTest, retainedPayloadKeepsPoolAlive) {
  constexpr int64_t kSize = 1'001;
  memory::MemoryManager manager;
  auto rootPool = manager.addRootPool("materialized_output");
  auto pool = rootPool->addLeafChild("payload");
  std::weak_ptr<memory::MemoryPool> weakPool = pool;
  auto sink = std::make_shared<RetainingExchangeSink>();
  std::unique_ptr<folly::IOBuf> clone;
  {
    MaterializedOutputBuffer buffer(1, sink, 1 << 20, 1 << 20, pool);
    auto data = buffer.allocateTrackedIOBuf(kSize);
    EXPECT_EQ(data->length(), 0);
    EXPECT_GE(data->capacity(), kSize);
    std::memset(data->writableData(), 'x', kSize);
    data->append(kSize);
    const auto allocatedBytes = pool->usedBytes();
    EXPECT_GE(allocatedBytes, kSize);
    buffer.enqueue(0, std::move(data));
    EXPECT_EQ(buffer.drainAll(), kSize);
    EXPECT_EQ(buffer.bufferedBytes(), 0);
    ASSERT_NE(sink->data_, nullptr);
    clone = sink->data_->clone();
    buffer.noMoreData();
    EXPECT_EQ(pool->usedBytes(), allocatedBytes);
  }
  sink->data_.reset();
  // Fail before dropping the last external pool reference if ownership is lost.
  ASSERT_GT(pool.use_count(), 1);
  pool.reset();
  ASSERT_FALSE(weakPool.expired());
  EXPECT_EQ(
      std::string_view(reinterpret_cast<const char*>(clone->data()), kSize),
      std::string(kSize, 'x'));
  EXPECT_GE(rootPool->usedBytes(), kSize);
  clone.reset();
  EXPECT_TRUE(weakPool.expired());
  EXPECT_EQ(rootPool->usedBytes(), 0);
}

TEST(MaterializedOutputBufferAllocationTest, allocationRespectsPoolLimit) {
  constexpr int64_t kCapacity = 1 << 20;
  memory::MemoryManager manager;
  auto rootPool = manager.addRootPool("materialized_output", kCapacity);
  auto pool = rootPool->addLeafChild("payload");
  auto sink = std::make_shared<RetainingExchangeSink>();
  MaterializedOutputBuffer buffer(1, sink, kCapacity, kCapacity, pool);
  VELOX_ASSERT_THROW(
      buffer.allocateTrackedIOBuf(2 * kCapacity),
      "Exceeded memory pool capacity");
  EXPECT_EQ(pool->usedBytes(), 0);
  EXPECT_EQ(buffer.bufferedBytes(), 0);
}

} // namespace
} // namespace facebook::velox::exec
