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

#include "velox/exec/window/PartitionStreamingWindowBuild.h"

namespace facebook::velox::exec::window {

PartitionStreamingWindowBuild::PartitionStreamingWindowBuild(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    velox::memory::MemoryPool* pool,
    const common::SpillConfig* spillConfig,
    tsan_atomic<bool>* nonReclaimableSection)
    : WindowBuild(windowNode, pool, spillConfig, nonReclaimableSection) {
  initializeRowContainer(pool);
  initializeDecodedInputVectors();
}

void PartitionStreamingWindowBuild::buildNextPartition() {
  partitionStartRows_.push_back(sortedRows_.size());
  sortedRows_.insert(sortedRows_.end(), inputRows_.begin(), inputRows_.end());
  inputRows_.clear();
}

void PartitionStreamingWindowBuild::addInput(RowVectorPtr input) {
  for (auto i = 0; i < inputChannels_.size(); ++i) {
    decodedInputVectors_[i].decode(*input->childAt(inputChannels_[i]));
  }

  for (auto row = 0; row < input->size(); ++row) {
    char* newRow = data_->newRow();

    for (auto col = 0; col < input->childrenSize(); ++col) {
      data_->store(decodedInputVectors_[col], row, newRow, col);
    }

    // Sorted input only guarantees that each partition's rows are contiguous:
    // the keys may be sorted descending, nulls last, or in another key order.
    // A partition therefore starts wherever any key changes, not only where
    // the keys increase.
    if (previousRow_ != nullptr &&
        !equalRowsOnKeys(previousRow_, newRow, partitionKeyInfo_)) {
      buildNextPartition();
    }

    inputRows_.push_back(newRow);
    previousRow_ = newRow;
  }
}

void PartitionStreamingWindowBuild::noMoreInput() {
  buildNextPartition();

  // Help for last partition related calculations.
  partitionStartRows_.push_back(sortedRows_.size());
}

std::shared_ptr<WindowPartition>
PartitionStreamingWindowBuild::nextPartition() {
  VELOX_CHECK_GT(
      partitionStartRows_.size(), 0, "No window partitions available");

  ++currentPartitionStartRowsIndex_;
  VELOX_CHECK_LE(
      currentPartitionStartRowsIndex_,
      partitionStartRows_.size() - 2,
      "All window partitions consumed");

  eraseConsumedRows();

  const auto partitionSize =
      partitionStartRows_[currentPartitionStartRowsIndex_ + 1] -
      partitionStartRows_[currentPartitionStartRowsIndex_];
  const auto partition = folly::Range(
      sortedRows_.data() + partitionStartRows_[currentPartitionStartRowsIndex_],
      partitionSize);

  return std::make_shared<WindowPartition>(
      data_.get(), partition, inversedInputChannels_, sortKeyInfo_);
}

void PartitionStreamingWindowBuild::eraseConsumedRows() {
  const auto currentStart =
      partitionStartRows_[currentPartitionStartRowsIndex_];
  if (currentStart > firstLiveRow_) {
    data_->eraseRows(
        folly::Range<char**>(
            sortedRows_.data() + firstLiveRow_, currentStart - firstLiveRow_));
    firstLiveRow_ = currentStart;
  }
  // Compacting moves only the live rows and partition starts, and runs only
  // once at least as many erased rows have accumulated, so its cost is
  // amortized over the erased rows. Dropping the starts of the earlier
  // partitions moves the current partition's start to index 0. needsInput()
  // and hasNextPartition() compare the index with partitionStartRows_.size(),
  // which shrinks by the same amount, so their results are unchanged.
  if (firstLiveRow_ == 0 ||
      2 * static_cast<size_t>(firstLiveRow_) < sortedRows_.size()) {
    return;
  }
  sortedRows_.erase(sortedRows_.cbegin(), sortedRows_.cbegin() + firstLiveRow_);
  sortedRows_.shrink_to_fit();
  partitionStartRows_.erase(
      partitionStartRows_.cbegin(),
      partitionStartRows_.cbegin() + currentPartitionStartRowsIndex_);
  for (auto& start : partitionStartRows_) {
    start -= firstLiveRow_;
  }
  currentPartitionStartRowsIndex_ = 0;
  firstLiveRow_ = 0;
}

bool PartitionStreamingWindowBuild::hasNextPartition() {
  return partitionStartRows_.size() > 0 &&
      currentPartitionStartRowsIndex_ <
      static_cast<vector_size_t>(partitionStartRows_.size() - 2);
}

} // namespace facebook::velox::exec::window
