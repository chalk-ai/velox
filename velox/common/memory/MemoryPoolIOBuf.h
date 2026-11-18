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

#pragma once

#include <cstdint>
#include <memory>

#include <folly/io/IOBuf.h>

namespace facebook::velox::memory {

class MemoryPool;

/// Allocates a contiguous IOBuf with zero length and capacity 'size' from a
/// non-null pool. Retains the pool and its allocation until the last reference
/// to the shared buffer, including IOBuf clones, is released. Allocations can
/// trigger memory arbitration and must run outside locks needed by reclaimers.
std::unique_ptr<folly::IOBuf> allocateIOBuf(
    const std::shared_ptr<MemoryPool>& pool,
    uint64_t size);

} // namespace facebook::velox::memory
