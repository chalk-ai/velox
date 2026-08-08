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
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {

namespace {

class NumericHistogramTest : public AggregationTestBase {
 protected:
  template <typename TValue>
  void runTwoArgsTests() {
    disableTestIncremental();
    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>({10, 20, 30, 80, 90, 100}),
      });

      auto expectedTwoBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{20, 3}, {90, 3}},
          }),
      });
      auto expectedFourBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{15, 2}, {30, 1}, {85, 2}, {100, 1}},
          }),
      });
      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{15, 2}, {30, 1}, {80, 1}, {90, 1}, {100, 1}},
          }),
      });

      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(5, c0)"},
          {expectedFiveBuckets});
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(2, c0)"},
          {expectedTwoBuckets});
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(4, c0)"},
          {expectedFourBuckets});
    }

    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>({10, 20, 30, 40, 50, 60, 70, 80}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{15.0, 2}, {35.0, 2}, {55.0, 2}, {70.0, 1}, {80.0, 1}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(5, c0)"},
          {expectedFiveBuckets});
    }

    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>({10, 50, 60, 70, 80, 90, 100}),
      });
      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{10.0, 1}, {55.0, 2}, {100.0, 1}, {75.0, 2}, {90.0, 1}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(5, c0)"},
          {expectedFiveBuckets});
    }

    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>({10}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{10, 1}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(5, c0)"},
          {expectedFiveBuckets});
    }

    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>({10, 20, 30, 40, 50, 60, 70, 80, 90}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{15, 2}, {35.0, 2}, {55, 2}, {75, 2}, {90.0, 1}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(5, c0)"},
          {expectedFiveBuckets});
    }

    {
      auto valuesAndWeights = makeRowVector({makeNullableFlatVector<TValue>(
          {1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0})});

      auto expectedFourBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{1.0, 5}, {2.0, 5}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(4, c0)"},
          {expectedFourBuckets});
    }

    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>(
              {0, 0, 0, 1, 0, 0, 1, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0,
               1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
               0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 2, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0,
               0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 1, 0,
               0, 0, 0, 0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
               0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
               0, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{0, 285}, {1, 31}, {2, 5}},
          }),
      });

      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(5, c0)"},
          {expectedFiveBuckets});
    }

    {
      auto variousBuckets = makeRowVector({
          makeNullableFlatVector<int64_t>({4, 3, 1, 4, 2, 1, 3, 2}),
          makeNullableFlatVector<TValue>(
              {1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{1.0, 4}, {2.0, 4}},
          }),
      });
      testAggregations(
          {variousBuckets},
          {},
          {"numeric_histogram(c0, c1)"},
          {expectedFiveBuckets});
    }
    {
      auto variousBuckets = makeRowVector({
          makeNullableFlatVector<int64_t>({5, 3, 1, 4, 2, 1, 3, 2, 1}),
          makeNullableFlatVector<TValue>({10, 20, 30, 40, 50, 60, 70, 80, 90}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{15, 2}, {35.0, 2}, {55, 2}, {75, 2}, {90.0, 1}},
          }),
      });
      testAggregations(
          {variousBuckets},
          {},
          {"numeric_histogram(c0, c1)"},
          {expectedFiveBuckets});
    }
    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>(
              {std::numeric_limits<TValue>::quiet_NaN()}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{std::numeric_limits<TValue>::quiet_NaN(), 1}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(2, c0)"},
          {expectedFiveBuckets});
    }
    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>(
              {2, std::numeric_limits<TValue>::quiet_NaN(), 2, 3}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{std::numeric_limits<TValue>::quiet_NaN(), 1}, {2, 2}, {3, 1}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(3, c0)"},
          {expectedFiveBuckets});
    }
    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>(
              {2,
               std::numeric_limits<TValue>::quiet_NaN(),
               2,
               std::numeric_limits<TValue>::infinity(),
               2,
               std::numeric_limits<TValue>::infinity()}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{std::numeric_limits<TValue>::quiet_NaN(), 1},
               {2, 3},
               {std::numeric_limits<TValue>::infinity(), 2}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(3, c0)"},
          {expectedFiveBuckets});
    }
    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>(
              {std::nullopt,
               2,
               std::numeric_limits<TValue>::quiet_NaN(),
               2,
               std::numeric_limits<TValue>::infinity(),
               2,
               std::nullopt,
               std::numeric_limits<TValue>::infinity()}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{std::numeric_limits<TValue>::quiet_NaN(), 1},
               {2, 3},
               {std::numeric_limits<TValue>::infinity(), 2}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(3, c0)"},
          {expectedFiveBuckets});
    }
  }

  template <typename TValue, typename TWeight>
  void runThreeArgsTests() {
    disableTestIncremental();
    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>({10, 20, 30, 80, 90, 100}),
          makeNullableFlatVector<TWeight>({2, 3, 1, 2, 1, 4}),
      });

      auto expectedTwoBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{18.333333333333332, 6}, {92.85714285714286, 7}},
          }),
      });
      auto expectedFourBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{10, 2}, {22.5, 4}, {83.33333333333333, 3}, {100, 4}},
          }),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{10, 2}, {20, 3}, {30, 1}, {100, 4}, {83.33333333333333, 3}},
          }),
      });

      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(2, c0, c1)"},
          {expectedTwoBuckets});
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(4, c0, c1)"},
          {expectedFourBuckets});
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(5, c0, c1)"},
          {expectedFiveBuckets});
    }

    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>({10, 20, 30, 40, 50, 60, 70, 80}),
          makeNullableFlatVector<TWeight>({2, 3, 1, 4, 2, 1, 3, 2}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{10.0, 2},
               {40.0, 4},
               {53.333333333333336, 3},
               {22.5, 4},
               {74.0, 5}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(5, c0, c1)"},
          {expectedFiveBuckets});
    }

    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>({10, 50, 60, 70, 80, 90, 100}),
          makeNullableFlatVector<TWeight>({2, 2, 1, 3, 2, 1, 4}),
      });
      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{10.0, 2},
               {53.333333333333336, 3},
               {100.0, 4},
               {70.0, 3},
               {83.33333333333333, 3}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(5, c0, c1)"},
          {expectedFiveBuckets});
    }

    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>({10}),
          makeNullableFlatVector<TWeight>({2}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{10, 2}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(5, c0, c1)"},
          {expectedFiveBuckets});
    }

    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>({10, 20, 30, 40, 50, 60, 70, 80, 90}),
          makeNullableFlatVector<TWeight>({2, 3, 1, 4, 2, 1, 3, 2, 1}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{18.333333333333332, 6},
               {40.0, 4},
               {53.333333333333336, 3},
               {70, 3},
               {83.33333333333333, 3}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(5, c0, c1)"},
          {expectedFiveBuckets});
    }

    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>(
              {1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0}),
          makeNullableFlatVector<TWeight>({3, 5, 3, 5, 3, 5, 3, 5, 3, 5}),
      });

      auto expectedFourBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{1.0, 15}, {2.0, 25}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(4, c0, c1)"},
          {expectedFourBuckets});
    }
    {
      auto variousBuckets = makeRowVector({
          makeNullableFlatVector<int64_t>({4, 3, 1, 4, 2, 1, 3, 2}),
          makeNullableFlatVector<TValue>(
              {1.0, 2.0, 1.0, 2.0, 1.0, 2.0, 1.0, 2.0}),
          makeNullableFlatVector<TWeight>({1, 1, 1, 1, 1, 1, 1, 1}),

      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{1.0, 4}, {2.0, 4}},
          }),
      });
      testAggregations(
          {variousBuckets},
          {},
          {"numeric_histogram(c0, c1, c2)"},
          {expectedFiveBuckets});
    }
    {
      auto variousBuckets = makeRowVector({
          makeNullableFlatVector<int64_t>({5, 3, 1, 4, 2, 1, 3, 2, 1}),
          makeNullableFlatVector<TValue>({10, 20, 30, 40, 50, 60, 70, 80, 90}),
          makeNullableFlatVector<TWeight>({1, 1, 1, 1, 1, 1, 1, 1, 1}),

      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{15, 2}, {35.0, 2}, {55, 2}, {75, 2}, {90.0, 1}},
          }),
      });
      testAggregations(
          {variousBuckets},
          {},
          {"numeric_histogram(c0, c1, c2)"},
          {expectedFiveBuckets});
    }
    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>(
              {std::nullopt,
               2,
               std::numeric_limits<TValue>::quiet_NaN(),
               2,
               std::numeric_limits<TValue>::infinity(),
               2,
               std::nullopt,
               std::numeric_limits<TValue>::infinity()}),
          makeNullableFlatVector<TWeight>({1, 1, 1, 1, 1, 1, 1, 1}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{std::numeric_limits<TValue>::quiet_NaN(), 1},
               {2, 3},
               {std::numeric_limits<TValue>::infinity(), 2}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(3, c0, c1)"},
          {expectedFiveBuckets});
    }
    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>({1, 2, 3}),
          makeNullableFlatVector<TWeight>(
              {std::numeric_limits<TWeight>::quiet_NaN(), 1, 1}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{1, std::numeric_limits<TValue>::quiet_NaN()}, {2.5, 2}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(2, c0, c1)"},
          {expectedFiveBuckets});
    }
    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>(
              {std::nullopt,
               2,
               std::numeric_limits<TValue>::quiet_NaN(),
               2,
               std::numeric_limits<TValue>::infinity(),
               2,
               std::nullopt,
               std::numeric_limits<TValue>::infinity()}),
          makeNullableFlatVector<TWeight>(
              {1,
               std::nullopt,
               1,
               std::nullopt,
               1,
               std::nullopt,
               1,
               std::nullopt}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{std::numeric_limits<TValue>::quiet_NaN(), 1},
               {std::numeric_limits<TValue>::infinity(), 1}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(3, c0, c1)"},
          {expectedFiveBuckets});
    }
    // Java implementation has some correctness issues and will fail this test
    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>(
              {std::numeric_limits<TValue>::quiet_NaN(),
               2,
               std::numeric_limits<TValue>::quiet_NaN()}),
          makeNullableFlatVector<TWeight>({1, 1, 1}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{std::numeric_limits<TValue>::quiet_NaN(), 2}, {2, 1}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(2, c0, c1)"},
          {expectedFiveBuckets});
    }
    {
      auto valuesAndWeights = makeRowVector({
          makeNullableFlatVector<TValue>({1, -1, 50}),
          makeNullableFlatVector<TWeight>({-1, 1, 1}),
      });

      auto expectedFiveBuckets = makeRowVector({
          makeMapVector<TValue, TValue>({
              {{-std::numeric_limits<TValue>::infinity(), 0}, {50, 1}},
          }),
      });
      testAggregations(
          {valuesAndWeights},
          {},
          {"numeric_histogram(2, c0, c1)"},
          {expectedFiveBuckets});
    }
  }
};

TEST_F(NumericHistogramTest, twoArgsDouble) {
  runTwoArgsTests<double>();
}

TEST_F(NumericHistogramTest, twoArgsFloat) {
  runTwoArgsTests<float>();
}

TEST_F(NumericHistogramTest, threeArgsDoubleDouble) {
  runThreeArgsTests<double, double>();
}

TEST_F(NumericHistogramTest, threeArgsDoubleFloat) {
  runThreeArgsTests<double, float>();
}

TEST_F(NumericHistogramTest, threeArgsFloatDouble) {
  runThreeArgsTests<float, double>();
}

TEST_F(NumericHistogramTest, threeArgsFloatFloat) {
  runThreeArgsTests<float, float>();
}

// Verify all-null input produces null output without crashing.
// Exercises the mergeSameBuckets fix for nextIndex_ == 0.
TEST_F(NumericHistogramTest, allNullInputTwoArgs) {
  disableTestIncremental();
  auto valuesAndWeights = makeRowVector({
      makeNullableFlatVector<double>(
          {std::nullopt, std::nullopt, std::nullopt}),
  });

  auto expected = makeRowVector({
      makeNullableMapVector<double, double>({std::nullopt}),
  });
  testAggregations(
      {valuesAndWeights}, {}, {"numeric_histogram(3, c0)"}, {expected});
}

// Verify all-null input produces null output for 3-arg variant.
TEST_F(NumericHistogramTest, allNullInputThreeArgs) {
  disableTestIncremental();
  auto valuesAndWeights = makeRowVector({
      makeNullableFlatVector<double>(
          {std::nullopt, std::nullopt, std::nullopt}),
      makeNullableFlatVector<double>({1, 2, 3}),
  });

  auto expected = makeRowVector({
      makeNullableMapVector<double, double>({std::nullopt}),
  });
  testAggregations(
      {valuesAndWeights}, {}, {"numeric_histogram(3, c0, c1)"}, {expected});
}

// Verify grouped aggregation where one group has all-null values.
// Exercises the empty histogram code path in compact().
TEST_F(NumericHistogramTest, groupedWithEmptyGroup) {
  disableTestIncremental();
  auto data = makeRowVector({
      // group keys: group 0 gets all nulls, group 1 gets real values
      makeNullableFlatVector<int32_t>({0, 0, 1, 1, 1}),
      makeNullableFlatVector<double>(
          {std::nullopt, std::nullopt, 10.0, 20.0, 30.0}),
  });

  auto expected = makeRowVector({
      makeNullableFlatVector<int32_t>({0, 1}),
      makeNullableMapVector<double, double>(
          {std::nullopt, {{{10.0, 1}, {20.0, 1}, {30.0, 1}}}}),
  });
  testAggregations({data}, {"c0"}, {"numeric_histogram(3, c1)"}, {expected});
}

// Stress-test with a large number of entries that forces multiple compact
// cycles. Validates unique_ptr cleanup in mergeAndReduceBuckets.
TEST_F(NumericHistogramTest, largeInputStressTest) {
  disableTestIncremental();
  // Create 200 identical values. This forces multiple compact+reduce cycles
  // with only 2 buckets, then mergeSameBuckets collapses them all.
  std::vector<std::optional<double>> values(200, 42.0);

  auto valuesAndWeights = makeRowVector({
      makeNullableFlatVector<double>(values),
  });

  auto expected = makeRowVector({
      makeMapVector<double, double>({
          {{42.0, 200}},
      }),
  });
  testAggregations(
      {valuesAndWeights}, {}, {"numeric_histogram(2, c0)"}, {expected});
}

// Verify duplicate values are merged correctly by mergeSameBuckets.
TEST_F(NumericHistogramTest, allDuplicateValues) {
  disableTestIncremental();
  auto valuesAndWeights = makeRowVector({
      makeNullableFlatVector<double>({5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0}),
  });

  auto expected = makeRowVector({
      makeMapVector<double, double>({
          {{5.0, 8}},
      }),
  });
  testAggregations(
      {valuesAndWeights}, {}, {"numeric_histogram(3, c0)"}, {expected});
}

// The auto-generated companions (numeric_histogram_partial / _merge /
// _merge_extract_*) invoke the aggregate factory with only the varbinary
// intermediate type, never the parent's (bigint, value) arguments. Verify
// that path builds a usable aggregate rather than failing the arity check.
// Buckets >= the number of distinct values keeps the histogram exact, so the
// expectation does not depend on how the harness splits rows across partials.
TEST_F(NumericHistogramTest, companionFunctionsDouble) {
  disableTestIncremental();
  auto data = makeRowVector({
      makeNullableFlatVector<double>({1.0, 2.0, 3.0, 4.0}),
  });

  auto expected = makeRowVector({
      makeMapVector<double, double>({
          {{1.0, 1}, {2.0, 1}, {3.0, 1}, {4.0, 1}},
      }),
  });

  testAggregationsWithCompanion(
      {data},
      [](auto& /*builder*/) {},
      {},
      {"numeric_histogram(4, c0)"},
      {{BIGINT(), DOUBLE()}},
      {},
      {expected});
}

// Same, for the REAL value type: it resolves to a differently suffixed
// merge_extract companion (map(real,real)) and a different adapter
// instantiation than the DOUBLE case above.
TEST_F(NumericHistogramTest, companionFunctionsReal) {
  disableTestIncremental();
  auto data = makeRowVector({
      makeNullableFlatVector<float>({1.0, 2.0, 3.0, 4.0}),
  });

  auto expected = makeRowVector({
      makeMapVector<float, float>({
          {{1.0, 1}, {2.0, 1}, {3.0, 1}, {4.0, 1}},
      }),
  });

  testAggregationsWithCompanion(
      {data},
      [](auto& /*builder*/) {},
      {},
      {"numeric_histogram(4, c0)"},
      {{BIGINT(), REAL()}},
      {},
      {expected});
}

} // namespace
} // namespace facebook::velox::aggregate::test
