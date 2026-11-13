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

#include <atomic>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/PrestoMaterializedPartitionedOutput.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/serializers/PrestoSerializer.h"

using namespace facebook::velox;

namespace facebook::velox::exec::test {
namespace {

constexpr std::string_view kTransport{"presto-materialized-test"};

struct RecordingSinkState {
  explicit RecordingSinkState(int32_t numPartitions)
      : partitionData(numPartitions) {}

  std::vector<std::string> snapshotData() const {
    std::lock_guard<std::mutex> lock(mutex);
    return partitionData;
  }

  std::vector<int64_t> snapshotPageSizes() const {
    std::lock_guard<std::mutex> lock(mutex);
    return pageSizes;
  }

  mutable std::mutex mutex;
  std::vector<std::string> partitionData;
  std::vector<int64_t> pageSizes;
  std::atomic_bool finished{false};
  std::atomic_bool aborted{false};
};

class RecordingExchangeSink : public ExchangeSink {
 public:
  explicit RecordingExchangeSink(std::shared_ptr<RecordingSinkState> state)
      : state_(std::move(state)) {}

  void append(int32_t partition, std::string_view data) override {
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->partitionData.at(partition).append(data);
  }

  void append(int32_t partition, std::unique_ptr<folly::IOBuf> data) override {
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->pageSizes.push_back(data->computeChainDataLength());
    data->coalesce();
    state_->partitionData.at(partition).append(
        reinterpret_cast<const char*>(data->data()), data->length());
  }

  CommittedExchangeOutput finish() override {
    state_->finished = true;
    CommittedExchangeOutput output;
    for (int32_t partition = 0;
         partition < static_cast<int32_t>(state_->partitionData.size());
         ++partition) {
      output.locations.emplace(
          partition, fmt::format("partition:{}", partition));
    }
    return output;
  }

  void abort() override {
    state_->aborted = true;
  }

  folly::F14FastMap<std::string, int64_t> stats() const override {
    return {};
  }

 private:
  const std::shared_ptr<RecordingSinkState> state_;
};

class PrestoMaterializedPartitionedOutputTest : public OperatorTestBase {
 protected:
  struct RunResult {
    std::shared_ptr<Task> task;
    std::shared_ptr<RecordingSinkState> sink;
  };

  RunResult runPlan(
      core::PlanNodePtr plan,
      int32_t numPartitions,
      int32_t numDrivers = 1,
      std::unordered_map<std::string, std::string> queryConfigs = {},
      bool expectFailure = false) {
    auto sink = std::make_shared<RecordingSinkState>(numPartitions);
    auto manager = std::make_shared<MaterializedOutputBufferManager>(
        [sink](const std::string&, const std::string&, memory::MemoryPool*)
            -> std::shared_ptr<ExchangeSink> {
          return std::make_shared<RecordingExchangeSink>(sink);
        },
        64L << 20);
    auto registry = OutputTransportRegistry::create(nullptr);
    registry->insert(
        std::string(kTransport),
        makePrestoMaterializedOutputTransportEntry(manager));

    auto queryCtx = core::QueryCtx::create(
        driverExecutor_.get(), core::QueryConfig{std::move(queryConfigs)});
    queryCtx->setRegistry(OutputTransportRegistry::kRegistryKey, registry);
    auto task = Task::create(
        fmt::format("presto-materialized-output-test-{}", nextTaskId_++),
        core::PlanFragment{std::move(plan)},
        0,
        std::move(queryCtx),
        Task::ExecutionMode::kParallel,
        Consumer{});
    task->start(numDrivers);
    if (expectFailure) {
      EXPECT_TRUE(waitForTaskFailure(task.get(), 10'000'000));
    } else {
      EXPECT_TRUE(waitForTaskCompletion(task.get(), 10'000'000));
    }
    return {std::move(task), std::move(sink)};
  }

  RunResult runExchangeWrite(
      const std::vector<RowVectorPtr>& data,
      int32_t numPartitions,
      int32_t numDrivers = 1,
      bool replicateNullsAndAny = false,
      std::unordered_map<std::string, std::string> queryConfigs = {}) {
    const std::vector<std::string> keys = numPartitions == 1
        ? std::vector<std::string>{}
        : std::vector<std::string>{"c0"};
    auto plan = PlanBuilder()
                    .values(data, true)
                    .partitionedOutput(
                        keys,
                        numPartitions,
                        replicateNullsAndAny,
                        {},
                        "Presto",
                        std::string(kTransport))
                    .planNode();
    return runPlan(
        std::move(plan), numPartitions, numDrivers, std::move(queryConfigs));
  }

  std::vector<RowVectorPtr> deserialize(
      const std::shared_ptr<RecordingSinkState>& sink,
      const RowTypePtr& rowType,
      common::CompressionKind compressionKind = common::CompressionKind_NONE) {
    std::vector<RowVectorPtr> results;
    serializer::presto::PrestoVectorSerde::PrestoOptions options;
    options.compressionKind = compressionKind;
    auto* serde = getNamedVectorSerde("Presto");
    for (auto& data : sink->snapshotData()) {
      if (data.empty()) {
        continue;
      }
      ByteRange range{
          reinterpret_cast<uint8_t*>(data.data()),
          static_cast<int32_t>(data.size()),
          0};
      BufferInputStream stream({range});
      RowVectorPtr result;
      vector_size_t resultOffset = 0;
      while (!stream.atEnd()) {
        serde->deserialize(
            &stream, pool(), rowType, &result, resultOffset, &options);
        resultOffset = result->size();
      }
      results.push_back(std::move(result));
    }
    return results;
  }

  void assertPagesFlushedBeforeCompletion(
      const std::vector<RowVectorPtr>& data) {
    auto sink = std::make_shared<RecordingSinkState>(1);
    auto manager = std::make_shared<MaterializedOutputBufferManager>(
        [sink](const std::string&, const std::string&, memory::MemoryPool*)
            -> std::shared_ptr<ExchangeSink> {
          return std::make_shared<RecordingExchangeSink>(sink);
        },
        64L << 20);
    auto plan = PlanBuilder()
                    .values(data)
                    .partitionedOutput(
                        {}, 1, false, {}, "Presto", std::string(kTransport))
                    .planNode();
    auto task = Task::create(
        fmt::format("presto-page-limits-test-{}", nextTaskId_++),
        core::PlanFragment{plan},
        0,
        core::QueryCtx::create(driverExecutor_.get()),
        Task::ExecutionMode::kParallel);
    manager->initializeTask(
        task, core::PartitionedOutputNode::Kind::kPartitioned, 1, 1);
    DriverCtx ctx(task, 0, 0, 0, 0);
    PrestoMaterializedPartitionedOutput output(
        1,
        &ctx,
        std::dynamic_pointer_cast<const core::PartitionedOutputNode>(plan),
        manager);
    for (const auto& batch : data) {
      output.addInput(batch);
    }
    // Only drain pages already serialized by addInput(), without noMoreInput().
    manager->buffer(task->taskId())->drainAll();
    EXPECT_FALSE(sink->finished);
    auto serialized = sink->snapshotData().front();
    ASSERT_FALSE(serialized.empty());
    ByteRange range{
        reinterpret_cast<uint8_t*>(serialized.data()),
        static_cast<int32_t>(serialized.size()),
        0};
    BufferInputStream stream({range});
    while (!stream.atEnd()) {
      RowVectorPtr page;
      getNamedVectorSerde("Presto")->deserialize(
          &stream, pool(), asRowType(data.front()->type()), &page);
      EXPECT_GT(page->size(), 0);
      EXPECT_LE(page->size(), 10'000);
    }
    output.close();
    manager->removeTask(task->taskId());
  }

  static inline std::atomic_int64_t nextTaskId_{0};
};

TEST_F(PrestoMaterializedPartitionedOutputTest, basicEndToEnd) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      makeFlatVector<std::string>({"a", "bb", "ccc", "dddd", "eeeee", "f"}),
  });

  auto result = runExchangeWrite({data}, 4, 2);
  EXPECT_TRUE(result.sink->finished);
  assertEqualResults(
      {data, data}, deserialize(result.sink, asRowType(data->type())));

  bool foundOperator = false;
  for (const auto& pipeline : result.task->taskStats().pipelineStats) {
    for (const auto& op : pipeline.operatorStats) {
      foundOperator |= op.operatorType == "PrestoMaterializedPartitionedOutput";
    }
  }
  EXPECT_TRUE(foundOperator);
}

TEST_F(PrestoMaterializedPartitionedOutputTest, manySmallInputBatches) {
  constexpr int32_t kNumBatches = 256;
  std::vector<RowVectorPtr> data;
  data.reserve(kNumBatches);
  for (int32_t batch = 0; batch < kNumBatches; ++batch) {
    data.push_back(makeRowVector({
        makeFlatVector<int64_t>({batch}),
        makeFlatVector<std::string>({fmt::format("value-{}", batch)}),
    }));
  }

  auto result = runExchangeWrite(data, 1);
  assertEqualResults(
      data, deserialize(result.sink, asRowType(data.front()->type())));
}

TEST_F(
    PrestoMaterializedPartitionedOutputTest,
    boundsPagesForZeroEstimateRows) {
  for (const vector_size_t batchSize : {25'000, 1'000}) {
    SCOPED_TRACE(batchSize);
    const std::vector<RowVectorPtr> inputs{
        makeRowVector(
            {makeFlatVector<std::string>(batchSize, [](auto) { return ""; })}),
        makeRowVector({makeNullableFlatVector<int64_t>(
            std::vector<std::optional<int64_t>>(batchSize))}),
        std::make_shared<RowVector>(
            pool(), ROW({}, {}), nullptr, batchSize, std::vector<VectorPtr>{}),
    };
    for (const auto& input : inputs) {
      SCOPED_TRACE(input->type()->toString());
      const std::vector<RowVectorPtr> data(25'000 / batchSize, input);
      assertPagesFlushedBeforeCompletion(data);
      auto result = runExchangeWrite(data, 1);
      assertEqualResults(
          data, deserialize(result.sink, asRowType(input->type())));
    }
  }
}

TEST_F(PrestoMaterializedPartitionedOutputTest, flushesOnSerializerMemory) {
  // Fewer than 10,000 rows with zero payload estimates. Multiple empty string
  // columns still grow offset buffers past the memory target.
  std::vector<VectorPtr> columns;
  for (int32_t column = 0; column < 8; ++column) {
    columns.push_back(
        makeFlatVector<std::string>(1'000, [](auto) { return ""; }));
  }
  auto input = makeRowVector(columns);
  const std::vector<RowVectorPtr> data(6, input);
  assertPagesFlushedBeforeCompletion(data);
  auto result = runExchangeWrite(data, 1);
  assertEqualResults(data, deserialize(result.sink, asRowType(input->type())));
}

TEST_F(PrestoMaterializedPartitionedOutputTest, replicateNullsAndAny) {
  auto data = makeRowVector({
      makeNullableFlatVector<int32_t>({1, 2, std::nullopt, 4, std::nullopt, 6}),
      makeFlatVector<std::string>({"a", "b", "c", "d", "e", "f"}),
  });

  auto result = runExchangeWrite({data}, 4, 1, true);
  std::map<std::string, int32_t> counts;
  for (const auto& vector : deserialize(result.sink, asRowType(data->type()))) {
    auto* values = vector->childAt(1)->as<SimpleVector<StringView>>();
    for (vector_size_t row = 0; row < vector->size(); ++row) {
      ++counts[values->valueAt(row).str()];
    }
  }

  EXPECT_EQ(counts["a"], 4);
  EXPECT_EQ(counts["c"], 4);
  EXPECT_EQ(counts["e"], 4);
  EXPECT_EQ(counts["b"], 1);
  EXPECT_EQ(counts["d"], 1);
  EXPECT_EQ(counts["f"], 1);
}

TEST_F(PrestoMaterializedPartitionedOutputTest, zeroColumnOutput) {
  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6})});
  auto sourceType = asRowType(data->type());
  auto source = PlanBuilder().values({data}).planNode();
  std::vector<core::TypedExprPtr> keys{
      std::make_shared<core::FieldAccessTypedExpr>(
          sourceType->childAt(0), sourceType->nameOf(0))};
  auto plan = std::make_shared<core::PartitionedOutputNode>(
      "presto-materialized-output",
      core::PartitionedOutputNode::Kind::kPartitioned,
      keys,
      4,
      false,
      std::make_shared<HashPartitionFunctionSpec>(
          sourceType, std::vector<column_index_t>{0}),
      ROW({}, {}),
      "Presto",
      std::string(kTransport),
      std::move(source));

  auto result = runPlan(std::move(plan), 4);
  auto rows = deserialize(result.sink, ROW({}, {}));
  int64_t numRows = 0;
  for (const auto& vector : rows) {
    EXPECT_EQ(vector->childrenSize(), 0);
    numRows += vector->size();
  }
  EXPECT_EQ(numRows, data->size());
}

TEST_F(PrestoMaterializedPartitionedOutputTest, compression) {
  constexpr int32_t kNumRows = 10'000;
  auto data = makeRowVector({
      makeFlatVector<int32_t>(kNumRows, [](auto row) { return row; }),
      makeFlatVector<std::string>(
          kNumRows, [](auto) { return std::string(64, 'x'); }),
  });

  auto result = runExchangeWrite(
      {data},
      1,
      1,
      false,
      {{core::QueryConfig::kShuffleCompressionKind, "lz4"}});
  assertEqualResults(
      {data},
      deserialize(
          result.sink, asRowType(data->type()), common::CompressionKind_LZ4));
}

TEST_F(PrestoMaterializedPartitionedOutputTest, rejectsNonPrestoSerde) {
  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});
  auto plan =
      PlanBuilder()
          .values({data})
          .partitionedOutput(
              {"c0"}, 2, false, {}, "CompactRow", std::string(kTransport))
          .planNode();

  auto result = runPlan(std::move(plan), 2, 1, {}, /*expectFailure=*/true);
  EXPECT_NE(
      result.task->errorMessage().find(
          "PrestoMaterializedPartitionedOutput requires Presto"),
      std::string::npos);
}

} // namespace
} // namespace facebook::velox::exec::test
