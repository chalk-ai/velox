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

#include "velox/exec/PrestoMaterializedPartitionedOutput.h"

#include <algorithm>
#include <cstring>
#include <numeric>

#include "velox/common/compression/Compression.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/PartitionedOutput.h"

namespace facebook::velox::exec {

namespace {

constexpr int32_t kMinOutputStreamSize = 128;
// Payload estimates can be zero for empty strings, nulls and zero-column rows.
constexpr vector_size_t kMaxPageRows = 10'000;

} // namespace

PrestoMaterializedPartitionedOutput::PrestoMaterializedPartitionedOutput(
    int32_t operatorId,
    DriverCtx* ctx,
    const std::shared_ptr<const core::PartitionedOutputNode>& planNode,
    const std::shared_ptr<MaterializedOutputBufferManager>& manager)
    : MaterializedPartitionedOutputBase(
          operatorId,
          ctx,
          planNode,
          manager,
          "PrestoMaterializedPartitionedOutput"),
      pageMaxBytes_(buffer_->partitionDrainThreshold()),
      serde_(getNamedVectorSerde("Presto")),
      serdeOptions_(getVectorSerdeOptions(
          common::stringToCompressionKind(
              ctx->queryConfig().shuffleCompressionKind()),
          "Presto",
          PartitionedOutput::minCompressionRatio(),
          ctx->queryConfig().minShuffleCompressionPageSizeBytes())),
      allRows_(pool()),
      destinations_(numDestinations_) {
  VELOX_USER_CHECK_EQ(
      planNode->serdeKind(),
      "Presto",
      "PrestoMaterializedPartitionedOutput requires Presto serialization");
  partitionRows_.resize(numDestinations_);
}

void PrestoMaterializedPartitionedOutput::estimateRowSizes(int32_t numRows) {
  rowSizes_.assign(numRows, 0);
  sizePointers_.resize(numRows);
  allRows_.resize(numRows);
  std::iota(allRows_.begin(), allRows_.end(), 0);
  for (vector_size_t row = 0; row < numRows; ++row) {
    sizePointers_[row] = &rowSizes_[row];
  }
  serde_->estimateSerializedSize(
      output_.get(),
      folly::Range<const vector_size_t*>(allRows_.data(), numRows),
      sizePointers_.data(),
      scratch_);
}

void PrestoMaterializedPartitionedOutput::selectPartitionRows(
    int32_t numRows,
    const std::vector<int32_t>& rowsToReplicate) {
  for (auto& rows : partitionRows_) {
    rows.clear();
  }

  if (broadcast_) {
    for (auto& rows : partitionRows_) {
      rows.assign(allRows_.begin(), allRows_.end());
    }
    return;
  }

  size_t replicaIndex = 0;
  for (vector_size_t row = 0; row < numRows; ++row) {
    if (replicaIndex < rowsToReplicate.size() &&
        row == rowsToReplicate[replicaIndex]) {
      for (auto& rows : partitionRows_) {
        rows.push_back(row);
      }
      ++replicaIndex;
    } else {
      partitionRows_[partitions_[row]].push_back(row);
    }
  }
}

void PrestoMaterializedPartitionedOutput::ensureStreamGroup(
    int32_t partition,
    int32_t numRows) {
  auto& destination = destinations_[partition];
  if (destination.streamGroup != nullptr) {
    return;
  }
  destination.streamGroup = std::make_unique<VectorStreamGroup>(pool(), serde_);
  destination.streamGroup->createStreamTree(
      outputType_, numRows, serdeOptions_.get());
}

void PrestoMaterializedPartitionedOutput::appendRows(
    int32_t partition,
    const std::vector<vector_size_t>& rows) {
  auto& destination = destinations_[partition];
  size_t begin = 0;
  while (begin < rows.size()) {
    if (destination.numRows >= kMaxPageRows ||
        (destination.numRows > 0 &&
         destination.estimatedBytes + rowSizes_[rows[begin]] > pageMaxBytes_)) {
      flushDestination(partition);
    }

    const auto chunkBegin = begin;
    const auto chunkEnd = std::min(
        rows.size(),
        begin + static_cast<size_t>(kMaxPageRows - destination.numRows));
    int64_t chunkBytes = 0;
    while (begin < chunkEnd) {
      const auto rowBytes = rowSizes_[rows[begin]];
      if (begin > chunkBegin &&
          destination.estimatedBytes + chunkBytes + rowBytes > pageMaxBytes_) {
        break;
      }
      chunkBytes += rowBytes;
      ++begin;
      if (destination.estimatedBytes + chunkBytes >= pageMaxBytes_) {
        break;
      }
    }

    const auto chunkSize = begin - chunkBegin;
    ensureStreamGroup(partition, chunkSize);
    destination.streamGroup->append(
        output_,
        folly::Range<const vector_size_t*>(rows.data() + chunkBegin, chunkSize),
        scratch_);
    destination.estimatedBytes += chunkBytes;
    destination.numRows += chunkSize;
    pendingEstimatedBytes_ += chunkBytes;

    // Include serializer allocations such as offsets and null bitmaps, which
    // the payload estimates may omit. This is a memory target, not an exact
    // serialized-page size limit, and can be exceeded by one bounded append.
    if (destination.numRows >= kMaxPageRows ||
        destination.estimatedBytes >= pageMaxBytes_ ||
        destination.streamGroup->size() >= static_cast<size_t>(pageMaxBytes_)) {
      flushDestination(partition);
    }
  }
}

void PrestoMaterializedPartitionedOutput::flushDestination(int32_t partition) {
  auto& destination = destinations_[partition];
  if (destination.streamGroup == nullptr || destination.numRows == 0) {
    return;
  }

  const auto estimatedBytes = destination.estimatedBytes;

  IOBufOutputStream stream(
      *pool(),
      nullptr,
      std::max<int64_t>(kMinOutputStreamSize, destination.streamGroup->size()));
  destination.streamGroup->flush(&stream);

  for (const auto& [name, counter] : destination.streamGroup->runtimeStats()) {
    auto [it, inserted] = destination.serdeStats.try_emplace(name, counter);
    if (!inserted) {
      it->second.value += counter.value;
    }
  }

  auto serialized = stream.getIOBuf();
  const auto serializedBytes = serialized->computeChainDataLength();
  auto tracked = buffer_->allocateTrackedIOBuf(serializedBytes);
  auto* destinationBytes = tracked->writableData();
  for (const auto range : *serialized) {
    std::memcpy(destinationBytes, range.data(), range.size());
    destinationBytes += range.size();
  }
  tracked->append(serializedBytes);
  buffer_->enqueue(partition, std::move(tracked));

  destination.streamGroup.reset();
  destination.estimatedBytes = 0;
  destination.numRows = 0;
  pendingEstimatedBytes_ -= estimatedBytes;
  VELOX_DCHECK_GE(pendingEstimatedBytes_, 0);
}

void PrestoMaterializedPartitionedOutput::flushBatch() {
  for (int32_t partition = 0; partition < numDestinations_; ++partition) {
    flushDestination(partition);
  }
}

void PrestoMaterializedPartitionedOutput::serializeInput(
    int32_t numRows,
    const std::vector<int32_t>& rowsToReplicate) {
  estimateRowSizes(numRows);
  selectPartitionRows(numRows, rowsToReplicate);

  for (int32_t partition = 0; partition < numDestinations_; ++partition) {
    appendRows(partition, partitionRows_[partition]);
  }

  if (pendingEstimatedBytes_ >= targetSizeInBytes_) {
    flushBatch();
  }
}

void PrestoMaterializedPartitionedOutput::recordSerdeStats() {
  for (const auto& destination : destinations_) {
    for (const auto& [name, counter] : destination.serdeStats) {
      addRuntimeStat(name, counter);
    }
  }
  auto lockedStats = stats().wlock();
  lockedStats->addRuntimeStat(
      Operator::kShuffleSerdeKind,
      RuntimeCounter(
          static_cast<int64_t>(VectorSerde::kindByName(serde_->kind()))));
  lockedStats->addRuntimeStat(
      Operator::kShuffleCompressionKind,
      RuntimeCounter(static_cast<int64_t>(serdeOptions_->compressionKind)));
}

std::shared_ptr<OutputTransportEntry>
makePrestoMaterializedOutputTransportEntry(
    std::shared_ptr<MaterializedOutputBufferManager> manager) {
  return OutputTransportEntry::make<MaterializedOutputBufferManager>(
      std::move(manager),
      [](int32_t operatorId,
         DriverCtx* ctx,
         const std::shared_ptr<const core::PartitionedOutputNode>& node,
         bool /*eagerFlush*/,
         const std::shared_ptr<MaterializedOutputBufferManager>& manager) {
        return std::make_unique<PrestoMaterializedPartitionedOutput>(
            operatorId, ctx, node, manager);
      });
}

} // namespace facebook::velox::exec
