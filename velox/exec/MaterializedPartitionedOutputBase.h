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

#include "velox/core/PlanNode.h"
#include "velox/exec/MaterializedOutputBufferManager.h"
#include "velox/exec/Operator.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec {

/// Shares routing, input preparation and lifecycle across materialized output
/// formats. Subclasses own serialization and operator-local batching.
class MaterializedPartitionedOutputBase : public Operator {
 public:
  void addInput(RowVectorPtr input) final;

  RowVectorPtr getOutput() final;

  bool needsInput() const final {
    return !finished_ && blockingReason_ == BlockingReason::kNotBlocked;
  }

  void noMoreInput() final;

  BlockingReason isBlocked(ContinueFuture* future) final;

  bool isFinished() final;

  void close() final;

 protected:
  MaterializedPartitionedOutputBase(
      int32_t operatorId,
      DriverCtx* ctx,
      const std::shared_ptr<const core::PartitionedOutputNode>& planNode,
      const std::shared_ptr<MaterializedOutputBufferManager>& manager,
      std::string_view operatorType);

  // Serializes output_ using partitions_ and the sorted input rows to replicate
  // to every destination. May flush format-specific batches as they fill.
  virtual void serializeInput(
      int32_t numRows,
      const std::vector<int32_t>& rowsToReplicate) = 0;

  // Emits all locally staged data into the shared buffer.
  virtual void flushBatch() = 0;

  virtual void recordSerdeStats() {}

  const int32_t numDestinations_;
  const bool broadcast_;
  const std::shared_ptr<MaterializedOutputBuffer> buffer_;
  const int64_t targetSizeInBytes_;

  RowVectorPtr output_;
  std::vector<uint32_t> partitions_;

 private:
  void initializeInput(RowVectorPtr input);

  void computePartitions(const RowVector& input, int32_t numRows);

  void collectNullRows(const RowVector& input, int32_t numRows);

  std::vector<int32_t> selectRowsToReplicate(int32_t numRows);

  void finish();

  void recordBufferStats();

  bool shouldReplicate() const {
    return !broadcast_ && replicateNullsAndAny_ && numDestinations_ > 1;
  }

  const std::vector<column_index_t> outputChannels_;
  const std::vector<column_index_t> keyChannels_;
  std::unique_ptr<core::PartitionFunction> partitionFunction_;
  const bool replicateNullsAndAny_;

  BlockingReason blockingReason_{BlockingReason::kNotBlocked};
  ContinueFuture future_;
  bool finished_{false};
  bool isLastDriver_{false};

  SelectivityVector rows_;
  SelectivityVector nullRows_;
  std::vector<DecodedVector> decodedVectors_;
  bool replicatedAny_{false};
};

} // namespace facebook::velox::exec
