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

#include "velox/dwio/parquet/common/BitStreamUtilsInternal.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace facebook::velox::parquet {
namespace {

// Writes 'numValues' bit-packed values of 'numBits' each and reads them back
// in batches of the given sizes. Reading in batches whose bit size is not a
// multiple of 8 leaves the reader at a sub-byte position, which the bulk
// unpack path must carry over exactly to the next batch.
template <typename T>
void testBatchedReads(
    int numBits,
    int numValues,
    const std::vector<int>& batchSizes) {
  std::vector<T> expected(numValues);
  for (int i = 0; i < numValues; ++i) {
    expected[i] = static_cast<T>((i * 7 + 1) & ((1u << numBits) - 1));
  }

  std::vector<uint8_t> buffer(numValues * 8 + 64);
  BitWriter writer(buffer.data(), buffer.size());
  for (int i = 0; i < numValues; ++i) {
    ASSERT_TRUE(writer.PutValue(expected[i], numBits));
  }
  writer.Flush();

  BitReader reader(buffer.data(), buffer.size());
  std::vector<T> actual(numValues);
  int numRead = 0;
  for (const int batchSize : batchSizes) {
    ASSERT_EQ(
        batchSize, reader.GetBatch(numBits, actual.data() + numRead, batchSize));
    numRead += batchSize;
  }
  ASSERT_EQ(numValues, numRead);

  EXPECT_THAT(actual, testing::ElementsAreArray(expected));
}

TEST(BitStreamUtilsTest, batchOnByteBoundary) {
  // Batch bit sizes that are multiples of 8 keep the reader byte-aligned.
  for (const int numBits : {1, 3, 5, 7, 12}) {
    testBatchedReads<uint32_t>(numBits, 1'024, {512, 512});
  }
}

TEST(BitStreamUtilsTest, batchOffByteBoundary) {
  // A first batch whose bit size is not a multiple of 8 leaves the reader at
  // a sub-byte position. The bulk unpack must not truncate it away.
  for (const int numBits : {1, 3, 5, 7, 12}) {
    for (const int firstBatch : {13, 21, 501}) {
      testBatchedReads<uint32_t>(numBits, 1'024, {firstBatch, 1'024 - firstBatch});
    }
  }
}

TEST(BitStreamUtilsTest, batchOffByteBoundaryNarrowOutput) {
  // Outputs narrower than 4 bytes take the buffered conversion path.
  for (const int numBits : {1, 3, 5, 7, 12}) {
    for (const int firstBatch : {13, 21, 501}) {
      testBatchedReads<uint16_t>(numBits, 1'024, {firstBatch, 1'024 - firstBatch});
    }
  }
}

TEST(BitStreamUtilsTest, manySmallUnalignedBatches) {
  // Every batch ends off a byte boundary, so each read starts at a sub-byte
  // position and exercises the unaligned prologue plus the bulk unpack.
  testBatchedReads<uint32_t>(3, 1'023, std::vector<int>(31, 33));
}

TEST(BitStreamUtilsTest, batchSmallerThanEight) {
  // Batches below eight values bypass the bulk unpack entirely.
  testBatchedReads<uint32_t>(3, 21, {5, 7, 3, 6});
}

} // namespace
} // namespace facebook::velox::parquet
