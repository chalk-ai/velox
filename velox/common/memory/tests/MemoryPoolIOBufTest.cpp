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

#include "velox/common/memory/MemoryPoolIOBuf.h"

#include <cstring>
#include <string>
#include <string_view>

#include <gtest/gtest.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"

namespace facebook::velox::memory {
namespace {

TEST(MemoryPoolIOBufTest, writableBufferTracksAlignedAllocation) {
  constexpr uint64_t kSize = 1'001;
  MemoryManager manager;
  auto rootPool = manager.addRootPool("iobuf");
  auto pool = rootPool->addLeafChild("payload");
  auto buffer = allocateIOBuf(pool, kSize);
  EXPECT_EQ(buffer->length(), 0);
  EXPECT_EQ(buffer->capacity(), kSize);
  EXPECT_GE(pool->usedBytes(), kSize);
  std::memset(buffer->writableData(), 'x', kSize);
  buffer->append(kSize);
  EXPECT_EQ(
      std::string_view(reinterpret_cast<const char*>(buffer->data()), kSize),
      std::string(kSize, 'x'));
  buffer.reset();
  EXPECT_EQ(pool->usedBytes(), 0);
}

TEST(MemoryPoolIOBufTest, clonesRetainPoolAndAllocation) {
  constexpr uint64_t kSize = 4'096;
  MemoryManager manager;
  auto rootPool = manager.addRootPool("iobuf");
  auto pool = rootPool->addLeafChild("payload");
  std::weak_ptr<MemoryPool> weakPool = pool;
  auto buffer = allocateIOBuf(pool, kSize);
  std::memset(buffer->writableData(), 'x', kSize);
  buffer->append(kSize);
  auto clone = buffer->clone();
  auto lastClone = clone->clone();
  buffer.reset();
  clone.reset();
  pool.reset();
  ASSERT_FALSE(weakPool.expired());
  EXPECT_EQ(rootPool->usedBytes(), kSize);
  EXPECT_EQ(lastClone->length(), kSize);
  EXPECT_EQ(lastClone->data()[kSize - 1], 'x');
  lastClone.reset();
  EXPECT_TRUE(weakPool.expired());
  EXPECT_EQ(rootPool->usedBytes(), 0);
}

TEST(MemoryPoolIOBufTest, allocationFailurePreservesErrorAndReleasesOwner) {
  constexpr int64_t kCapacity = 1 << 20;
  MemoryManager manager;
  auto rootPool = manager.addRootPool("iobuf", kCapacity);
  auto pool = rootPool->addLeafChild("payload");
  std::weak_ptr<MemoryPool> weakPool = pool;
  try {
    allocateIOBuf(pool, 2 * kCapacity);
    FAIL() << "Expected a memory capacity error";
  } catch (const VeloxRuntimeError& error) {
    EXPECT_EQ(error.errorCode(), error_code::kMemCapExceeded);
  }
  EXPECT_EQ(rootPool->usedBytes(), 0);
  pool.reset();
  EXPECT_TRUE(weakPool.expired());
}

TEST(MemoryPoolIOBufTest, rejectsNullPool) {
  EXPECT_THROW(allocateIOBuf(nullptr, 1'024), VeloxRuntimeError);
}

} // namespace
} // namespace facebook::velox::memory
