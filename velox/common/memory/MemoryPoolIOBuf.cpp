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

#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/MemoryPool.h"

namespace facebook::velox::memory {
namespace {

// Keeps allocation metadata alive until the shared IOBuf storage is released.
struct BufferOwner {
  // Retains the pool independently of the allocating object's lifetime.
  std::shared_ptr<MemoryPool> pool;
  // Records the requested size before pool alignment for the matching free.
  uint64_t size;
};

} // namespace

std::unique_ptr<folly::IOBuf> allocateIOBuf(
    const std::shared_ptr<MemoryPool>& pool,
    uint64_t size) {
  VELOX_CHECK_NOT_NULL(pool);
  auto owner = std::make_unique<BufferOwner>(pool, size);
  auto* data = pool->allocate(size);
  // takeOwnership invokes the deleter on construction failure as well.
  return folly::IOBuf::takeOwnership(
      data,
      size,
      0,
      [](void* buffer, void* userData) {
        const std::unique_ptr<BufferOwner> owner{
            static_cast<BufferOwner*>(userData)};
        owner->pool->free(buffer, owner->size);
      },
      owner.release());
}

} // namespace facebook::velox::memory
