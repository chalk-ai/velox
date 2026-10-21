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

#include "velox/exec/window/WindowBuild.h"

namespace facebook::velox::exec::window {

/// The PartitionStreamingWindowBuild is used when the input data is already
/// sorted by {partition keys + order by keys}. The logic identifies partition
/// changes when receiving input rows and splits out WindowPartitions for the
/// Window operator to process.
class PartitionStreamingWindowBuild : public WindowBuild {
 public:
  PartitionStreamingWindowBuild(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      velox::memory::MemoryPool* pool,
      const common::SpillConfig* spillConfig,
      tsan_atomic<bool>* nonReclaimableSection);

  void addInput(RowVectorPtr input) override;

  void spill() override {
    VELOX_UNREACHABLE();
  }

  std::optional<exec::SpillStats> spilledStats() const override {
    return std::nullopt;
  }

  void noMoreInput() override;

  bool hasNextPartition() override;

  std::shared_ptr<WindowPartition> nextPartition() override;

  bool needsInput() override {
    // No partitions are available or the current partition is the last
    // available one, so can consume input rows.
    return partitionStartRows_.empty() ||
        currentPartitionStartRowsIndex_ == partitionStartRows_.size() - 2;
  }

 private:
  void buildNextPartition();

  // Erases the rows of the partitions before currentPartitionStartRowsIndex_
  // from data_, and compacts sortedRows_ and partitionStartRows_ once the
  // erased prefix is at least as long as the rest, so erasing costs amortized
  // O(1) per row.
  void eraseConsumedRows();

  // Vector of pointers to each input row in the data_ RowContainer.
  // Rows are erased from data_ when they are output from the
  // Window operator. Entries before firstLiveRow_ are already erased.
  std::vector<char*> sortedRows_;

  // Index in sortedRows_ of the first row not yet erased from data_.
  vector_size_t firstLiveRow_ = 0;

  // Holds input rows within the current partition.
  std::vector<char*> inputRows_;

  // Indices of  the start row (in sortedRows_) of each partition in
  // the RowContainer data_. This auxiliary structure helps demarcate
  // partitions.
  std::vector<vector_size_t> partitionStartRows_;

  // Used to compare rows based on partitionKeys.
  char* previousRow_ = nullptr;

  // Index in partitionStartRows_ of the start of the partition being output,
  // or -1 before the first. Used to construct WindowPartitions during
  // resetPartition.
  vector_size_t currentPartitionStartRowsIndex_ = -1;
};

} // namespace facebook::velox::exec::window
