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

#include "velox/expression/VectorReaders.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

using namespace facebook::velox::test;

class ArrayShuffleTest : public SparkFunctionBaseTest {
 protected:
  // The exact permutation depends on the standard library's shuffle, so check
  // the contract instead: per-row element multisets are unchanged and the
  // result is deterministic for a given seed and partition.
  void testShuffle(
      const VectorPtr& input,
      int64_t seed,
      int32_t partitionId = 0) {
    const auto sql = fmt::format("shuffle(c0, {})", seed);
    auto rows = makeRowVector({input});

    setSparkPartitionId(partitionId);
    auto result = evaluate(sql, rows);

    setSparkPartitionId(partitionId);
    assertEqualVectors(result, evaluate(sql, rows));

    setSparkPartitionId(partitionId);
    assertEqualVectors(
        evaluate(fmt::format("sort_array({})", sql), rows),
        evaluate("sort_array(c0)", rows));
  }

  template <typename T>
  void testShuffle(
      const std::string& input,
      int64_t seed,
      int32_t partitionId = 0) {
    testShuffle(makeArrayVectorFromJson<T>({input}), seed, partitionId);
  }
};

TEST_F(ArrayShuffleTest, basic) {
  testShuffle<int64_t>("[1, 2, 3, 4, 5]", 0);
  testShuffle<std::string>(R"(["a", "b", "c", "d"])", 0);
  testShuffle<int64_t>("[1, 2, 3, 4, 5]", 0, 1);
  testShuffle<int64_t>("[1, 2, 3, 4, 5]", 2, 0);
}

TEST_F(ArrayShuffleTest, seedAndPartitionChangeResult) {
  // With 20 distinct elements an unchanged permutation would mean the input,
  // seed, or partition id is ignored.
  auto rows = makeRowVector({makeArrayVectorFromJson<int64_t>(
      {"[1, 2, 3, 4, 5, 6, 7, 8, 9, 10,"
       " 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]"})});

  setSparkPartitionId(0);
  auto seed0 = evaluate("shuffle(c0, 0)", rows);
  setSparkPartitionId(0);
  auto seed2 = evaluate("shuffle(c0, 2)", rows);
  setSparkPartitionId(1);
  auto partition1 = evaluate("shuffle(c0, 0)", rows);

  ASSERT_FALSE(seed0->equalValueAt(rows->childAt(0).get(), 0, 0));
  ASSERT_FALSE(seed0->equalValueAt(seed2.get(), 0, 0));
  ASSERT_FALSE(seed0->equalValueAt(partition1.get(), 0, 0));
}

TEST_F(ArrayShuffleTest, nestedArrays) {
  auto input = makeNestedArrayVectorFromJson<int64_t>(
      {"[[1, 2, 3, 4], [5, 6]]",
       "[null, null, [1, 2, 3, 4], [5, 6], [6, 7, 8]]",
       "[[]]",
       "[[null]]"});
  testShuffle(input, 0);
}

TEST_F(ArrayShuffleTest, constantEncoding) {
  vector_size_t size = 3;
  // Test empty array, array with null element,
  // array with duplicate elements, and array with distinct values.
  auto valueVector = makeArrayVectorFromJson<int64_t>(
      {"[]", "[null, 0]", "[5, 5]", "[1, 2, 3]"});
  for (auto i = 0; i < valueVector->size(); i++) {
    auto input = BaseVector::wrapInConstant(size, i, valueVector);
    testShuffle(input, 0);
  }
}

TEST_F(ArrayShuffleTest, dictEncoding) {
  // Test dict with repeated elements: {1,2,3} x 3, {4,5} x 2.
  auto base = makeArrayVectorFromJson<int64_t>(
      {"[0]",
       "[1, 2 ,3]",
       "[4, 5, null]",
       "[1, 2, 3]",
       "[1, 2, 3]",
       "[4, 5, null]"});
  // Test repeated index elements and indices filtering (filter out element at
  // index 0).
  auto indices = makeIndices({3, 3, 4, 2, 2, 1, 1, 1});
  auto input = wrapInDictionary(indices, base);
  testShuffle(input, 0);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
