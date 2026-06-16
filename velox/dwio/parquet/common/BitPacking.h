/* Copyright (c) Facebook, Inc. and its affiliates.
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

// Adapted from Apache Arrow:
// https://github.com/apache/arrow/blob/main/cpp/src/arrow/util/bpacking_internal.h
// Copyright 2016-2024 The Apache Software Foundation

#pragma once

#include "arrow/util/visibility.h"

#include <cstdint>

// Mirrors the declarations from arrow's private header
// `arrow/util/bpacking_internal.h`.
namespace arrow::internal {

struct UnpackOptions {
  int batch_size;
  int bit_width;
  int bit_offset = 0;
  int max_read_bytes = -1;
};

template <typename Uint>
ARROW_EXPORT void unpack(const uint8_t* in, Uint* out, const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void unpack<uint32_t>( //
    const uint8_t* in,
    uint32_t* out,
    const UnpackOptions& opts);

extern template ARROW_TEMPLATE_EXPORT void unpack<uint64_t>( //
    const uint8_t* in,
    uint64_t* out,
    const UnpackOptions& opts);

} // namespace arrow::internal
