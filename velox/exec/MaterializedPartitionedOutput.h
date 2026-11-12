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

#include "velox/exec/MaterializedPartitionedOutputBase.h"
#include "velox/row/CompactRow.h"

namespace facebook::velox::exec {

/// Serializes PartitionedOutput rows as CompactRow RowGroups and writes them
/// through a MaterializedOutputBuffer.
class MaterializedPartitionedOutput : public MaterializedPartitionedOutputBase {
 public:
  MaterializedPartitionedOutput(
      int32_t operatorId,
      DriverCtx* ctx,
      const std::shared_ptr<const core::PartitionedOutputNode>& planNode,
      const std::shared_ptr<MaterializedOutputBufferManager>& manager);

 private:
  void serializeInput(
      int32_t numRows,
      const std::vector<int32_t>& rowsToReplicate) override;

  // Appends the current output rows to the serialized staging buffers.
  void serializeRows(row::CompactRow& compactRow, int32_t numRows);

  // Serializes fixed-width rows directly into pre-sized staging storage.
  void serializeFixedWidthRows(row::CompactRow& compactRow, int32_t numRows);

  // Sizes variable-width rows before appending them to staging storage.
  void serializeVariableWidthRows(row::CompactRow& compactRow, int32_t numRows);

  // Grows staging storage while preserving rows already serialized into it.
  void ensureFlatBufferCapacity(int64_t additionalBytes);

  // Replicates row metadata across destinations without copying row bytes.
  void appendReplicaEntries(
      int32_t serializeStartRow,
      const std::vector<int32_t>& rowsToReplicate);

  // Groups staged rows by destination, emits bounded RowGroups, and resets.
  void flushBatch() override;

  // Encodes selected rows as one CompactRow RowGroup and enqueues it.
  void flushRowGroup(int32_t partition, std::vector<int32_t>& rowIndices);

  const int64_t rowGroupMaxBytes_;
  const std::optional<int32_t> fixedRowSize_;

  int32_t rowCount_{0};
  int64_t flatBufferSize_{0};
  BufferPtr flatBuffer_;
  std::vector<int64_t> rowOffsets_;
  std::vector<int32_t> rowSizes_;
  std::vector<uint32_t> rowPartitions_;
};

} // namespace facebook::velox::exec
