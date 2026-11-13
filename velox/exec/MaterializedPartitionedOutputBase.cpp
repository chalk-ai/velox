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

#include "velox/exec/MaterializedPartitionedOutputBase.h"

#include <algorithm>

#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"

namespace facebook::velox::exec {

MaterializedPartitionedOutputBase::MaterializedPartitionedOutputBase(
    int32_t operatorId,
    DriverCtx* ctx,
    const std::shared_ptr<const core::PartitionedOutputNode>& planNode,
    const std::shared_ptr<MaterializedOutputBufferManager>& manager,
    std::string_view operatorType)
    : Operator(
          ctx,
          planNode->outputType(),
          operatorId,
          planNode->id(),
          operatorType),
      numDestinations_(planNode->numPartitions()),
      broadcast_(planNode->isBroadcast()),
      buffer_(manager->buffer(ctx->task->taskId())),
      targetSizeInBytes_(manager->outputBatchSizeBytes(numDestinations_)),
      outputChannels_(calculateOutputChannels(
          planNode->inputType(),
          planNode->outputType(),
          planNode->outputType())),
      keyChannels_(toChannels(planNode->inputType(), planNode->keys())),
      partitionFunction_(
          broadcast_ || numDestinations_ == 1
              ? nullptr
              : planNode->partitionFunctionSpec().create(
                    numDestinations_,
                    false)),
      replicateNullsAndAny_(planNode->isReplicateNullsAndAny()) {
  VELOX_CHECK_GT(numDestinations_, 0);
  VELOX_CHECK_NOT_NULL(buffer_);
}

void MaterializedPartitionedOutputBase::initializeInput(RowVectorPtr input) {
  if (outputType_->size() == 0) {
    output_ = std::make_shared<RowVector>(
        input->pool(),
        outputType_,
        nullptr,
        input->size(),
        std::vector<VectorPtr>{});
  } else if (outputChannels_.empty()) {
    output_ = std::move(input);
  } else {
    std::vector<VectorPtr> outputColumns;
    outputColumns.reserve(outputChannels_.size());
    for (const auto channel : outputChannels_) {
      outputColumns.push_back(input->childAt(channel));
    }
    output_ = std::make_shared<RowVector>(
        input->pool(),
        outputType_,
        nullptr,
        input->size(),
        std::move(outputColumns));
  }

  for (column_index_t channel = 0; channel < output_->childrenSize();
       ++channel) {
    output_->childAt(channel)->loadedVector();
  }
}

void MaterializedPartitionedOutputBase::computePartitions(
    const RowVector& input,
    int32_t numRows) {
  partitions_.resize(numRows);
  if (broadcast_ || numDestinations_ == 1) {
    std::fill(partitions_.begin(), partitions_.end(), 0);
    return;
  }
  const auto singlePartition =
      partitionFunction_->partition(input, partitions_);
  if (singlePartition.has_value()) {
    std::fill(partitions_.begin(), partitions_.end(), singlePartition.value());
  }
}

void MaterializedPartitionedOutputBase::collectNullRows(
    const RowVector& input,
    int32_t numRows) {
  rows_.resize(numRows);
  rows_.setAll();
  nullRows_.resize(numRows);
  nullRows_.clearAll();
  decodedVectors_.resize(keyChannels_.size());

  for (size_t keyIndex = 0; keyIndex < keyChannels_.size(); ++keyIndex) {
    const auto keyChannel = keyChannels_[keyIndex];
    if (keyChannel == kConstantChannel) {
      continue;
    }
    const auto& keyVector = input.childAt(keyChannel);
    if (!keyVector->mayHaveNulls()) {
      continue;
    }
    auto& decoded = decodedVectors_[keyIndex];
    decoded.decode(*keyVector, rows_);
    if (const auto* rawNulls = decoded.nulls(&rows_)) {
      bits::orWithNegatedBits(
          nullRows_.asMutableRange().bits(), rawNulls, 0, numRows);
    }
  }
  nullRows_.updateBounds();
}

std::vector<int32_t> MaterializedPartitionedOutputBase::selectRowsToReplicate(
    int32_t numRows) {
  std::vector<int32_t> rowsToReplicate;
  int32_t firstNullRow = 0;
  if (!replicatedAny_) {
    rowsToReplicate.push_back(0);
    replicatedAny_ = true;
    firstNullRow = 1;
  }
  for (int32_t row = firstNullRow; row < numRows; ++row) {
    if (nullRows_.isValid(row)) {
      rowsToReplicate.push_back(row);
    }
  }
  return rowsToReplicate;
}

void MaterializedPartitionedOutputBase::addInput(RowVectorPtr input) {
  auto rawInput = input;
  initializeInput(std::move(input));
  VELOX_CHECK_NOT_NULL(output_);
  const auto numRows = output_->size();
  if (numRows == 0) {
    output_.reset();
    return;
  }

  computePartitions(*rawInput, numRows);
  std::vector<int32_t> rowsToReplicate;
  if (shouldReplicate()) {
    collectNullRows(*rawInput, numRows);
    rowsToReplicate = selectRowsToReplicate(numRows);
  }
  serializeInput(numRows, rowsToReplicate);
  output_.reset();
  blockingReason_ = buffer_->isBlocked(&future_);
}

RowVectorPtr MaterializedPartitionedOutputBase::getOutput() {
  return nullptr;
}

void MaterializedPartitionedOutputBase::noMoreInput() {
  Operator::noMoreInput();
  finish();
}

BlockingReason MaterializedPartitionedOutputBase::isBlocked(
    ContinueFuture* future) {
  if (blockingReason_ == BlockingReason::kNotBlocked) {
    return BlockingReason::kNotBlocked;
  }
  *future = std::move(future_);
  const auto reason = blockingReason_;
  blockingReason_ = BlockingReason::kNotBlocked;
  return reason;
}

bool MaterializedPartitionedOutputBase::isFinished() {
  return finished_;
}

void MaterializedPartitionedOutputBase::recordBufferStats() {
  for (const auto& [name, value] : buffer_->stats()) {
    addRuntimeStat(name, RuntimeCounter(value));
  }
}

void MaterializedPartitionedOutputBase::close() {
  if (!finished_) {
    buffer_->abort();
  }
  recordSerdeStats();
  if (isLastDriver_) {
    recordBufferStats();
  }
  Operator::close();
}

void MaterializedPartitionedOutputBase::finish() {
  if (finished_) {
    return;
  }
  flushBatch();

  std::vector<ContinuePromise> peerPromises;
  std::vector<std::shared_ptr<Driver>> peers;
  ContinueFuture peerFuture;
  auto* driverContext = operatorCtx()->driverCtx();
  const auto isLast = driverContext->task->allPeersFinished(
      planNodeId(), driverContext->driver, &peerFuture, peerPromises, peers);
  if (isLast) {
    // allPeersFinished returns true only for the last driver. That driver owns
    // the shared buffer's terminal snapshot and publishes it exactly once.
    isLastDriver_ = true;
    buffer_->noMoreData();
    driverContext->task->setAllOutputConsumed();
    for (auto& promise : peerPromises) {
      promise.setValue();
    }
  }
  finished_ = true;
}

} // namespace facebook::velox::exec
