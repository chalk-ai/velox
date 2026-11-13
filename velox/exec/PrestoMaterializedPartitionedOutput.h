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

#include <unordered_map>

#include "velox/exec/MaterializedPartitionedOutputBase.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::exec {

/// Serializes PartitionedOutput rows as Presto pages and writes them through a
/// MaterializedOutputBuffer. This is a separate opt-in path from
/// MaterializedPartitionedOutput, which continues to use CompactRow.
class PrestoMaterializedPartitionedOutput
    : public MaterializedPartitionedOutputBase {
 public:
  PrestoMaterializedPartitionedOutput(
      int32_t operatorId,
      DriverCtx* ctx,
      const std::shared_ptr<const core::PartitionedOutputNode>& planNode,
      const std::shared_ptr<MaterializedOutputBufferManager>& manager);

 private:
  struct DestinationState {
    std::unique_ptr<VectorStreamGroup> streamGroup;
    int64_t estimatedBytes{0};
    vector_size_t numRows{0};
    std::unordered_map<std::string, RuntimeCounter> serdeStats;
  };

  void serializeInput(
      int32_t numRows,
      const std::vector<int32_t>& rowsToReplicate) override;

  // Estimates the serialized size of every row in the current input.
  void estimateRowSizes(int32_t numRows);

  // Builds the selected row list for every destination.
  void selectPartitionRows(
      int32_t numRows,
      const std::vector<int32_t>& rowsToReplicate);

  // Appends selected rows to a destination, bounding page growth by row count,
  // estimated payload bytes and allocated serializer memory.
  void appendRows(int32_t partition, const std::vector<vector_size_t>& rows);

  // Creates the destination's Presto serializer on first use after a flush.
  void ensureStreamGroup(int32_t partition, int32_t numRows);

  // Flushes one destination's pending Presto page into the shared buffer.
  void flushDestination(int32_t partition);

  // Flushes all partial destination pages.
  void flushBatch() override;

  // Publishes Presto serializer counters from this driver.
  void recordSerdeStats() override;

  const int64_t pageMaxBytes_;
  VectorSerde* const serde_;
  const std::unique_ptr<VectorSerde::Options> serdeOptions_;

  std::vector<std::vector<vector_size_t>> partitionRows_;

  std::vector<vector_size_t> rowSizes_;
  std::vector<vector_size_t*> sizePointers_;
  raw_vector<vector_size_t> allRows_;
  Scratch scratch_;
  std::vector<DestinationState> destinations_;
  int64_t pendingEstimatedBytes_{0};
};

/// Creates an opt-in transport entry that uses Presto serialization while
/// sharing the existing materialized output manager, buffer and sink.
std::shared_ptr<OutputTransportEntry>
makePrestoMaterializedOutputTransportEntry(
    std::shared_ptr<MaterializedOutputBufferManager> manager);

} // namespace facebook::velox::exec
