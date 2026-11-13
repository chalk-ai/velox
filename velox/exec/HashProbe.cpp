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

#include "velox/exec/HashProbe.h"
#include <folly/ScopeGuard.h>
#include <map>
#include "velox/common/base/Counters.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/OperatorType.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/expression/FieldReference.h"
#include "velox/vector/LazyVector.h"

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::exec {

namespace {

// Batch size used when iterating the row container.
constexpr int kBatchSize = 1024;

template <typename T>
int64_t applyBloomFilter(
    const common::BigintValuesUsingBloomFilter& filter,
    const DecodedVector& decoded,
    SelectivityVector& rows) {
  int64_t numAccepted = 0;
  rows.applyToSelected([&](vector_size_t row) {
    if (filter.testInt64(decoded.valueAt<T>(row))) {
      ++numAccepted;
    } else {
      rows.setValid(row, false);
    }
  });
  return numAccepted;
}
} // namespace

// static
RowTypePtr HashProbe::makeTableType(
    const RowType* type,
    const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
        keys) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::unordered_set<column_index_t> keyChannels(keys.size());
  names.reserve(type->size());
  types.reserve(type->size());
  for (const auto& key : keys) {
    auto channel = type->getChildIdx(key->name());
    names.emplace_back(type->nameOf(channel));
    types.emplace_back(type->childAt(channel));
    keyChannels.insert(channel);
  }
  for (auto i = 0; i < type->size(); ++i) {
    if (keyChannels.find(i) == keyChannels.end()) {
      names.emplace_back(type->nameOf(i));
      types.emplace_back(type->childAt(i));
    }
  }
  return ROW(std::move(names), std::move(types));
}

namespace {
// Copy values from 'rows' of 'table' according to 'projections' in
// 'result'. Reuses 'result' children where possible.
void extractColumns(
    BaseHashTable* table,
    folly::Range<char* const*> rows,
    folly::Range<const IdentityProjection*> projections,
    memory::MemoryPool* pool,
    const std::vector<TypePtr>& resultTypes,
    std::vector<VectorPtr>& resultVectors) {
  VELOX_CHECK_EQ(resultTypes.size(), resultVectors.size());
  for (auto projection : projections) {
    const auto resultChannel = projection.outputChannel;
    VELOX_CHECK_LT(resultChannel, resultVectors.size());

    auto& child = resultVectors[resultChannel];
    // TODO: Consider reuse of complex types.
    if (!child || !BaseVector::isVectorWritable(child) ||
        !child->isFlatEncoding()) {
      child = BaseVector::create(resultTypes[resultChannel], rows.size(), pool);
    }
    child->resize(rows.size());
    table->extractColumn(rows, projection.inputChannel, child);
  }
}

BlockingReason fromStateToBlockingReason(ProbeOperatorState state) {
  switch (state) {
    case ProbeOperatorState::kRunning:
      [[fallthrough]];
    case ProbeOperatorState::kFinish:
      return BlockingReason::kNotBlocked;
    case ProbeOperatorState::kWaitForBuild:
      return BlockingReason::kWaitForJoinBuild;
    case ProbeOperatorState::kWaitForPeers:
      return BlockingReason::kWaitForJoinProbe;
    default:
      VELOX_UNREACHABLE("Unexpected state: ", probeOperatorStateName(state));
  }
}

template <typename T>
T* initBuffer(BufferPtr& buffer, vector_size_t size, memory::MemoryPool* pool) {
  VELOX_CHECK(!buffer || buffer->isMutable());
  if (!buffer || buffer->size() < size * sizeof(T)) {
    buffer = AlignedBuffer::allocate<T>(size, pool);
  }
  return buffer->asMutable<T>();
}
} // namespace

HashProbe::HashProbe(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::HashJoinNode>& joinNode)
    : Operator(
          driverCtx,
          joinNode->outputType(),
          operatorId,
          joinNode->id(),
          OperatorType::kHashProbe,
          joinNode->canSpill(driverCtx->queryConfig())
              ? driverCtx->makeSpillConfig(operatorId, OperatorType::kHashProbe)
              : std::nullopt),
      outputBatchSize_{outputBatchRows()},
      preferredOutputBatchBytes_{
          driverCtx->queryConfig().preferredOutputBatchBytes()},
      joinNode_(std::move(joinNode)),
      joinType_{joinNode_->joinType()},
      nullAware_{joinNode_->isNullAware()},
      nullAsValue_{joinNode_->isNullAsValue()},
      probeType_(joinNode_->sources()[0]->outputType()),
      canOutputBuildRowsInParallel_(
          driverCtx->queryConfig().parallelOutputJoinBuildRowsEnabled() &&
          !canSpill()),
      joinBridge_(operatorCtx_->task()->getHashJoinBridgeLocked(
          operatorCtx_->driverCtx()->splitGroupId,
          planNodeId())),
      bypassBloomFilterMinRows_{
          driverCtx->queryConfig().bypassHashProbeBloomFilterMinRows()},
      bypassBloomFilterMinPct_{
          driverCtx->queryConfig().bypassHashProbeBloomFilterMinPct()},
      bypassBloomFilter_{
          bypassBloomFilterMinRows_ <= 0 || bypassBloomFilterMinPct_ <= 0},
      filterResult_(1),
      outputTableRowsCapacity_(outputBatchSize_) {
  VELOX_CHECK_NOT_NULL(joinBridge_);
  VELOX_USER_CHECK_GE(bypassBloomFilterMinRows_, 0);
  VELOX_USER_CHECK_GE(bypassBloomFilterMinPct_, 0);
  VELOX_USER_CHECK_LE(bypassBloomFilterMinPct_, 100);
  // Mixed grouped execution restarts the probe per group over the same
  // spilled table partitions, which the legacy peer barrier coordinates.
  if (canSpill() &&
      !operatorCtx_->task()->hasMixedExecutionGroupJoin(joinNode_.get())) {
    sharedSpillRestore_ = true;
    hasSharedPartition_ = true;
    sharedWorkHeld_ = true;
    joinBridge_->addProber(needLastProbe());
  }
}

void HashProbe::initialize() {
  Operator::initialize();

  VELOX_CHECK(hashers_.empty());
  hashers_ = createVectorHashers(probeType_, joinNode_->leftKeys());

  const auto numKeys = hashers_.size();
  keyChannels_.reserve(numKeys);
  for (auto& hasher : hashers_) {
    keyChannels_.push_back(hasher->channel());
  }

  VELOX_CHECK_NULL(lookup_);
  lookup_ = std::make_unique<HashLookup>(hashers_, pool());
  auto buildType = joinNode_->sources()[1]->outputType();
  auto tableType = makeTableType(buildType.get(), joinNode_->rightKeys());
  if (joinNode_->filter()) {
    initializeFilter(joinNode_->filter(), probeType_, tableType);
  }

  size_t numIdentityProjections = 0;
  for (auto i = 0; i < probeType_->size(); ++i) {
    auto& name = probeType_->nameOf(i);
    auto outIndex = outputType_->getChildIdxIfExists(name);
    if (!outIndex.has_value()) {
      continue;
    }
    projectedInputColumns_[i] = *outIndex;
    if (!isRightJoin(joinType_) && !isFullJoin(joinType_)) {
      identityProjections_.emplace_back(i, *outIndex);
      if (*outIndex == i) {
        ++numIdentityProjections;
      }
    }
  }

  for (column_index_t i = 0; i < outputType_->size(); ++i) {
    auto tableChannel = tableType->getChildIdxIfExists(outputType_->nameOf(i));
    if (tableChannel.has_value()) {
      tableOutputProjections_.emplace_back(tableChannel.value(), i);
    }
  }

  if (numIdentityProjections == probeType_->size() &&
      tableOutputProjections_.empty()) {
    isIdentityProjection_ = true;
  }

  if (nullAware_) {
    filterTableResult_.resize(1);
  }
}

void HashProbe::initializeFilter(
    const core::TypedExprPtr& filter,
    const RowTypePtr& probeType,
    const RowTypePtr& tableType) {
  std::vector<core::TypedExprPtr> filters = {filter};
  filter_ =
      std::make_unique<ExprSet>(std::move(filters), operatorCtx_->execCtx());

  column_index_t filterChannel = 0;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  auto numFields = filter_->expr(0)->distinctFields().size();
  names.reserve(numFields);
  types.reserve(numFields);
  for (auto& field : filter_->expr(0)->distinctFields()) {
    const auto& name = field->field();
    auto channel = probeType->getChildIdxIfExists(name);
    if (channel.has_value()) {
      auto channelValue = channel.value();
      filterInputProjections_.emplace_back(channelValue, filterChannel++);
      names.emplace_back(probeType->nameOf(channelValue));
      types.emplace_back(probeType->childAt(channelValue));
      continue;
    }
    channel = tableType->getChildIdxIfExists(name);
    if (channel.has_value()) {
      auto channelValue = channel.value();
      filterTableProjections_.emplace_back(channelValue, filterChannel);
      names.emplace_back(tableType->nameOf(channelValue));
      types.emplace_back(tableType->childAt(channelValue));
      ++filterChannel;
      continue;
    }
    VELOX_FAIL(
        "Join filter field {} not in probe or build input", field->toString());
  }

  filterInputType_ = ROW(std::move(names), std::move(types));
}

void HashProbe::maybeSetupInputSpiller(
    const SpillPartitionIdSet& spillPartitionIds) {
  VELOX_CHECK_NULL(inputSpiller_);
  VELOX_CHECK(spillInputPartitionIds_.empty());

  spillInputPartitionIds_ = spillPartitionIds;
  if (spillInputPartitionIds_.empty()) {
    return;
  }

  const auto bitOffset = partitionBitOffset(
      *spillInputPartitionIds_.begin(),
      spillConfig()->startPartitionBit,
      spillConfig()->numPartitionBits);

  // If 'spillInputPartitionIds_' is not empty, then we set up a spiller to
  // spill the incoming probe inputs.
  inputSpiller_ = std::make_unique<NoRowContainerSpiller>(
      probeType_,
      restoringPartitionId_,
      HashBitRange(bitOffset, bitOffset + spillConfig()->numPartitionBits),
      spillConfig(),
      spillStats_.get());

  // Set the spill partitions to the corresponding ones at the build side. We
  // only spill the seen partitions from the build side. For the ones not seen
  // from the build side, we can directly produce results. The hash probe
  // operator itself won't trigger any spilling.
  inputSpiller_->setPartitionsSpilled(spillInputPartitionIds_);

  spillPartitionFunction_ = std::make_unique<SpillPartitionFunction>(
      SpillPartitionIdLookup(
          spillInputPartitionIds_,
          spillConfig()->startPartitionBit,
          spillConfig()->numPartitionBits),
      probeType_,
      keyChannels_);

  // If we have received no more input signal from either source or restored
  // spill input, then we shall just finish the spiller and records the spilled
  // partition set accordingly.
  if (noMoreSpillInput_) {
    inputSpiller_->finishSpill(inputSpillPartitionSet_);
  }
}

void HashProbe::maybeSetupSpillInputReader(
    const std::optional<SpillPartitionId>& restoredPartitionId) {
  VELOX_CHECK_NULL(spillInputReader_);
  if (!restoredPartitionId.has_value()) {
    return;
  }
  // If 'restoredPartitionId' is not null, then 'table_' is built from the
  // spilled build data. Create an unsorted reader to read the probe inputs from
  // the corresponding spilled probe partition on disk.
  auto iter = inputSpillPartitionSet_.find(restoredPartitionId.value());
  VELOX_CHECK(iter != inputSpillPartitionSet_.end());
  auto partition = std::move(iter->second);
  VELOX_CHECK_EQ(partition->id(), restoredPartitionId.value());
  restoringPartitionId_ = restoredPartitionId;
  if (sharedSpillRestore_) {
    restoringProbeFiles_ = partition->files();
  }
  spillInputReader_ = partition->createUnorderedReader(
      spillConfig_->readBufferSize, pool(), spillStats_.get());
  inputSpillPartitionSet_.erase(iter);
}

std::optional<uint64_t> HashProbe::estimatedRowSize(
    const std::vector<vector_size_t>& varSizedColumns,
    uint64_t totalFixedColumnsBytes) {
  static const double kToleranceRatio = 10.0;
  std::vector<RowColumn::Stats> varSizeListColumnsStats;
  varSizeListColumnsStats.reserve(varSizedColumns.size());
  for (uint32_t i = 0; i < varSizedColumns.size(); ++i) {
    const auto statsOpt = columnStats(varSizedColumns[i]);
    if (!statsOpt.has_value() || !statsOpt->minMaxColumnStatsValid()) {
      return std::nullopt;
    }
    varSizeListColumnsStats.push_back(statsOpt.value());
  }

  uint64_t totalAvgBytes{totalFixedColumnsBytes};
  uint64_t totalMaxBytes{totalFixedColumnsBytes};
  for (const auto& stats : varSizeListColumnsStats) {
    totalAvgBytes += stats.avgBytes();
    totalMaxBytes += stats.maxBytes();
  }
  if (totalAvgBytes == 0) {
    if (totalMaxBytes == 0) {
      return 0;
    }
    // Return nullopt to prevent memory exploding in extreme size skew cases:
    // e.g. 1 row very large and all other rows of size 0.
    return std::nullopt;
  }
  if (totalMaxBytes / totalAvgBytes >= kToleranceRatio) {
    return std::nullopt;
  }
  // Make the total per batch size to be bounded by 2x 'outputBatchSize_':
  // worst case size = (outputBatchSize_ / estimated size) * totalMaxBytes
  return (totalMaxBytes + totalAvgBytes) / 2;
}

std::optional<RowColumn::Stats> HashProbe::columnStats(
    int32_t columnIndex) const {
  std::vector<RowColumn::Stats> columnStats;
  const auto rowContainers = table_->allRows();
  for (const auto* rowContainer : rowContainers) {
    VELOX_CHECK_NOT_NULL(rowContainer);
    auto statsOpt = rowContainer->columnStats(columnIndex);
    if (!statsOpt.has_value()) {
      return std::nullopt;
    }
    columnStats.push_back(statsOpt.value());
  }
  return RowColumn::Stats::merge(columnStats);
}

void HashProbe::initializeResultIter() {
  VELOX_CHECK_NOT_NULL(table_);
  if (resultIter_ != nullptr) {
    return;
  }
  std::vector<vector_size_t> listColumns;
  listColumns.reserve(tableOutputProjections_.size());
  for (const auto& projection : tableOutputProjections_) {
    listColumns.push_back(projection.inputChannel);
  }
  std::vector<vector_size_t> varSizeListColumns;
  uint64_t fixedSizeListColumnsSizeSum{0};
  varSizeListColumns.reserve(tableOutputProjections_.size());
  for (const auto column : listColumns) {
    if (table_->rows()->columnTypes()[column]->isFixedWidth()) {
      fixedSizeListColumnsSizeSum += table_->rows()->fixedSizeAt(column);
    } else {
      varSizeListColumns.push_back(column);
    }
  }

  auto rowSizeEstimation =
      estimatedRowSize(varSizeListColumns, fixedSizeListColumnsSizeSum);
  // TODO: Make tolerance ratio configurable if needed.
  resultIter_ = std::make_unique<BaseHashTable::JoinResultIterator>(
      std::move(varSizeListColumns),
      fixedSizeListColumnsSizeSum,
      rowSizeEstimation);
}

void HashProbe::pushdownDynamicFilters() {
  auto* driver = operatorCtx_->driverCtx()->driver;
  const bool hashProbeStringDynamicFilterPushdownEnabled =
      operatorCtx_->driverCtx()
          ->queryConfig()
          .hashProbeStringDynamicFilterPushdownEnabled();
  auto numFilters = driver->pushdownFilters(
      this,
      keyChannels_,
      [&](column_index_t sourceChannel,
          std::shared_ptr<common::Filter>& filter) {
        if (dynamicFiltersProducedOnChannels_.contains(sourceChannel)) {
          return true;
        }
        auto& hasher = *table_->hashers()[sourceChannel];
        if (hasher.typeKind() == TypeKind::VARCHAR ||
            hasher.typeKind() == TypeKind::VARBINARY) {
          if (!hashProbeStringDynamicFilterPushdownEnabled) {
            return false;
          }
        }
        filter = hasher.getFilter(false);
        if (!filter) {
          filter = hasher.getBloomFilter();
          if (!filter) {
            return false;
          }
          auto* bloomFilter =
              checkedPointerCast<const common::BigintValuesUsingBloomFilter>(
                  filter.get());
          addRuntimeStat(
              std::string(HashProbe::kBloomFilterSize),
              RuntimeCounter(bloomFilter->blocksByteSize()));
        }
        dynamicFiltersProducedOnChannels_.insert(sourceChannel);
        for (auto* peer : findPeerOperators()) {
          peer->dynamicFiltersProducedOnChannels_.insert(sourceChannel);
        }
        return true;
      });
  // The join can be completely replaced with a pushed down filter when the
  // following conditions are met:
  //  * hash table has a single key with unique values,
  //  * build side has no dependent columns.
  if (keyChannels_.size() == 1 && !table_->hasDuplicateKeys() &&
      tableOutputProjections_.empty() && !filter_ && numFilters > 0 &&
      !table_->hashers()[0]->getBloomFilter() && !isRightJoin(joinType_)) {
    canReplaceWithDynamicFilter_ = true;
  }
}

void HashProbe::applyBloomFilterForJoinProbe() {
  // TODO: Add Bloom filter support for full joins.
  if (bypassBloomFilter_ || nullAware_ || isSpillInput() ||
      needToSpillInput() ||
      !(isLeftJoin(joinType_) || isLeftSemiProjectJoin(joinType_) ||
        isAntiJoin(joinType_))) {
    return;
  }

  bool hasBloomFilter = false;
  for (const auto& hasher : table_->hashers()) {
    if (hasher->getBloomFilter()) {
      hasBloomFilter = true;
      break;
    }
  }
  if (!hasBloomFilter) {
    bypassBloomFilter_ = true;
    return;
  }

  const int64_t numTested = activeRows_.countSelected();
  int64_t numAccepted = numTested;
  const bool isSampling = bloomFilterSampledRows_ < bypassBloomFilterMinRows_;
  for (auto i = 0; i < hashers_.size(); ++i) {
    const auto& filter = table_->hashers()[i]->getBloomFilter();
    if (!filter) {
      continue;
    }
    const auto* bloomFilter =
        checkedPointerCast<const common::BigintValuesUsingBloomFilter>(
            filter.get());
    const auto& decoded = hashers_[i]->decodedVector();
    switch (hashers_[i]->typeKind()) {
      case TypeKind::INTEGER:
        numAccepted =
            applyBloomFilter<int32_t>(*bloomFilter, decoded, activeRows_);
        break;
      case TypeKind::BIGINT:
        numAccepted =
            applyBloomFilter<int64_t>(*bloomFilter, decoded, activeRows_);
        break;
      default:
        VELOX_UNREACHABLE();
    }
  }
  activeRows_.updateBounds();

  if (isSampling) {
    bloomFilterSampledRows_ += numTested;
    bloomFilterSampleAcceptedRows_ += numAccepted;
    if (bloomFilterSampledRows_ >= bypassBloomFilterMinRows_) {
      bypassBloomFilter_ = bloomFilterSampleAcceptedRows_ * 100 >=
          bloomFilterSampledRows_ * bypassBloomFilterMinPct_;
      if (bypassBloomFilter_) {
        addRuntimeStat(std::string(kBloomFilterBypassed), RuntimeCounter(1));
      }
    }
  }

  if (numTested > 0) {
    addRuntimeStat(
        std::string(kBloomFilterTestedRows), RuntimeCounter(numTested));
    addRuntimeStat(
        std::string(kBloomFilterAcceptedRows), RuntimeCounter(numAccepted));
  }
}

void HashProbe::asyncWaitForHashTable() {
  checkRunning();
  VELOX_CHECK_NULL(table_);

  // Release any reserved memory before wait for next round of hash join in case
  // of disk spilling has been triggered.
  pool()->release();

  std::optional<HashJoinBridge::HashBuildResult> hashBuildResult;
  if (sharedSpillRestore_) {
    hashBuildResult = joinBridge_->acquireTable(
        sharedPartition_, sharedTableReserved_, &future_);
    sharedTableReserved_ = true;
  } else {
    hashBuildResult = joinBridge_->tableOrFuture(&future_);
  }
  if (!hashBuildResult.has_value()) {
    VELOX_CHECK(future_.valid());
    setState(ProbeOperatorState::kWaitForBuild);
    return;
  }

  if (sharedFinalizing_) {
    // A split table is empty; its children emit their own build-side rows.
    if (hashBuildResult->table == nullptr ||
        !hashBuildResult->spillPartitionIds.empty()) {
      finishSharedPartition();
      return;
    }
    table_ = std::move(hashBuildResult->table);
    restoringPartitionId_ = sharedPartition_;
    noMoreSpillInput_ = true;
    beginFinalizing();
    return;
  }

  if (hashBuildResult->hasNullKeys) {
    VELOX_CHECK(nullAware_);
    if (isAntiJoin(joinType_) && !joinNode_->filter()) {
      // Null-aware anti join with null keys on the build side without a filter
      // always returns nothing.
      // The flag must be set on the first (and only) built 'table_'.
      VELOX_CHECK(inputSpillPartitionSet_.empty());
      noMoreInput();
      return;
    }
    buildSideHasNullKeys_ = true;
  }

  table_ = std::move(hashBuildResult->table);
  initializeResultIter();

  VELOX_CHECK_NOT_NULL(table_);

  maybeSetupSpillInputReader(hashBuildResult->restoredPartitionId);
  maybeSetupInputSpiller(hashBuildResult->spillPartitionIds);
  checkMaxSpillLevel(hashBuildResult->restoredPartitionId);

  if (table_->numDistinct() == 0) {
    if (skipProbeOnEmptyBuild()) {
      if (!needToSpillInput()) {
        if (isSpillInput() ||
            operatorCtx_->driverCtx()
                ->queryConfig()
                .hashProbeFinishEarlyOnEmptyBuild()) {
          noMoreInput();
        } else {
          skipInput_ = true;
        }
      }
    }
  } else if (
      (isInnerJoin(joinType_) || isLeftSemiFilterJoin(joinType_) ||
       isCountingLeftSemiFilterJoin(joinType_) ||
       isRightSemiFilterJoin(joinType_) ||
       (isRightSemiProjectJoin(joinType_) && !nullAware_) ||
       isRightJoin(joinType_) || isRightAntiJoin(joinType_)) &&
      table_->hashMode() != BaseHashTable::HashMode::kHash && !isSpillInput() &&
      operatorCtx_->driverCtx()
          ->queryConfig()
          .hashProbeDynamicFilterPushdownEnabled() &&
      !hasMoreSpillData()) {
    // Find out whether there are any upstream operators that can accept dynamic
    // filters on all or a subset of the join keys. Create dynamic filters to
    // push down.
    //
    // NOTE: this optimization is not applied in the following cases: (1) if the
    // probe input is read from spilled data and there is no upstream operators
    // involved; (2) if there is spill data to restore, then we can't filter
    // probe inputs solely based on the current table's join keys.
    pushdownDynamicFilters();
  }
}

bool HashProbe::isSpillInput() const {
  return spillInputReader_ != nullptr;
}

void HashProbe::prepareForSpillRestore() {
  checkRunning();
  VELOX_CHECK(canSpill());
  VELOX_CHECK(hasMoreSpillData());

  // Reset the internal states which are relevant to the previous probe run.
  noMoreSpillInput_ = false;
  if (lastProber_) {
    table_->clear(true);
  }
  table_.reset();
  inputSpiller_.reset();
  spillInputReader_.reset();
  restoringPartitionId_.reset();
  spillInputPartitionIds_.clear();
  spillOutputReader_.reset();

  VELOX_CHECK(promises_.empty() || lastProber_);
  if (!lastProber_) {
    return;
  }
  // Notify the hash build operators to build the next hash table.
  joinBridge_->probeFinished();

  wakeupPeerOperators();

  lastProber_ = false;
}

void HashProbe::wakeupPeerOperators() {
  VELOX_CHECK(lastProber_);
  auto promises = std::move(promises_);
  for (auto& promise : promises) {
    promise.setValue();
  }
  promises_.clear();
}

std::vector<HashProbe*> HashProbe::findPeerOperators() {
  auto task = operatorCtx_->task();
  const std::vector<Operator*> operators =
      task->findPeerOperators(operatorCtx_->driverCtx()->pipelineId, this);
  std::vector<HashProbe*> probeOps;
  probeOps.reserve(operators.size());
  for (auto* op : operators) {
    auto* probeOp = dynamic_cast<HashProbe*>(op);
    probeOps.push_back(probeOp);
  }
  return probeOps;
}

void HashProbe::addSpillInput() {
  checkRunning();

  if (input_ != nullptr || noMoreSpillInput_) {
    return;
  }
  if (FOLLY_UNLIKELY(!spillInputReader_->nextBatch(input_))) {
    noMoreInputInternal();
    return;
  }

  addInput(std::move(input_));
}

void HashProbe::spillInput(RowVectorPtr& input) {
  VELOX_CHECK(needToSpillInput());

  const auto numInputRows = input->size();
  prepareInputIndicesBuffers(
      input->size(), inputSpiller_->state().spilledPartitionIdSet());

  spillPartitionFunction_->partition(*input, spillPartitions_);

  vector_size_t numNonSpillingInput = 0;
  for (auto row = 0; row < numInputRows; ++row) {
    const auto& partitionId = spillPartitions_[row];
    if (!inputSpiller_->state().isPartitionSpilled(partitionId)) {
      rawNonSpillInputIndicesBuffer_[numNonSpillingInput++] = row;
      continue;
    }
    rawSpillInputIndicesBuffers_.at(
        partitionId)[numSpillInputs_.at(partitionId)++] = row;
  }
  if (numNonSpillingInput == numInputRows) {
    return;
  }

  // Ensure vector are lazy loaded before spilling.
  for (int32_t i = 0; i < input->childrenSize(); ++i) {
    input->childAt(i)->loadedVector();
  }

  for (const auto& [partitionId, numRows] : numSpillInputs_) {
    if (numRows == 0) {
      continue;
    }
    VELOX_CHECK(inputSpiller_->state().isPartitionSpilled(partitionId));
    inputSpiller_->spill(
        partitionId,
        wrap(numRows, spillInputIndicesBuffers_.at(partitionId), input));
  }

  if (numNonSpillingInput == 0) {
    input = nullptr;
  } else {
    input = wrap(numNonSpillingInput, nonSpillInputIndicesBuffer_, input);
  }
}

void HashProbe::prepareInputIndicesBuffers(
    vector_size_t numInput,
    const SpillPartitionIdSet& spillPartitionIds) {
  VELOX_DCHECK(canSpill());
  const auto maxIndicesBufferBytes = numInput * sizeof(vector_size_t);
  if (nonSpillInputIndicesBuffer_ == nullptr ||
      !nonSpillInputIndicesBuffer_->isMutable() ||
      nonSpillInputIndicesBuffer_->size() < maxIndicesBufferBytes) {
    nonSpillInputIndicesBuffer_ = allocateIndices(numInput, pool());
    rawNonSpillInputIndicesBuffer_ =
        nonSpillInputIndicesBuffer_->asMutable<vector_size_t>();
  }
  spillInputIndicesBuffers_.clear();
  rawSpillInputIndicesBuffers_.clear();
  numSpillInputs_.clear();
  for (const auto& partitionId : spillPartitionIds) {
    if (!spillInputIndicesBuffers_.contains(partitionId) ||
        spillInputIndicesBuffers_.at(partitionId)->size() <
            maxIndicesBufferBytes) {
      spillInputIndicesBuffers_.insert_or_assign(
          partitionId, allocateIndices(numInput, pool()));
      rawSpillInputIndicesBuffers_.insert_or_assign(
          partitionId,
          spillInputIndicesBuffers_.at(partitionId)
              ->asMutable<vector_size_t>());
    }
    numSpillInputs_.insert_or_assign(partitionId, 0);
  }
}

BlockingReason HashProbe::isBlocked(ContinueFuture* future) {
  switch (state_) {
    case ProbeOperatorState::kWaitForBuild:
      VELOX_CHECK_NULL(table_);
      if (!future_.valid()) {
        setRunning();
        asyncWaitForHashTable();
      }
      break;
    case ProbeOperatorState::kRunning:
      VELOX_CHECK_NOT_NULL(table_);
      if (spillInputReader_ != nullptr) {
        addSpillInput();
      }
      break;
    case ProbeOperatorState::kWaitForPeers:
      if (!future_.valid()) {
        setRunning();
      }
      break;
    case ProbeOperatorState::kFinish:
      break;
    default:
      VELOX_UNREACHABLE(probeOperatorStateName(state_));
      break;
  }

  if (future_.valid()) {
    VELOX_CHECK(!isRunning());
    *future = std::move(future_);
  }
  return fromStateToBlockingReason(state_);
}

void HashProbe::decodeAndDetectNonNullKeys() {
  nonNullInputRows_.resize(input_->size());
  nonNullInputRows_.setAll();

  for (auto i = 0; i < hashers_.size(); ++i) {
    auto key = input_->childAt(hashers_[i]->channel())->loadedVector();
    hashers_[i]->decode(*key, nonNullInputRows_);
  }

  if (!nullAsValue_) {
    deselectRowsWithNulls(hashers_, nonNullInputRows_);
  }
  if (isRightSemiProjectJoin(joinType_) &&
      nonNullInputRows_.countSelected() < input_->size()) {
    probeSideHasNullKeys_ = true;
  }
}

void HashProbe::addInput(RowVectorPtr input) {
  if (skipInput_) {
    VELOX_CHECK_NULL(input_);
    return;
  }
  input_ = std::move(input);

  // Reset passingInputRowsInitialized_ as input_ as changed.
  passingInputRowsInitialized_ = false;

  const auto numInput = input_->size();

  if (numInput > 0) {
    noInput_ = false;
  }

  if (canReplaceWithDynamicFilter_) {
    replacedWithDynamicFilter_ = true;
    return;
  }

  bool hasDecoded = false;

  if (needToSpillInput()) {
    if (isRightSemiProjectJoin(joinType_) && !probeSideHasNullKeys_) {
      decodeAndDetectNonNullKeys();
      hasDecoded = true;
    }

    spillInput(input_);
    // Check if all the probe input rows have been spilled.
    if (input_ == nullptr) {
      return;
    }
  }

  if (table_->numDistinct() == 0) {
    if (skipProbeOnEmptyBuild()) {
      VELOX_CHECK(needToSpillInput());
      input_ = nullptr;
      return;
    }
    // Build side is empty. This state is valid only for anti, left and full
    // joins.
    VELOX_CHECK(joinIncludesMissesFromLeft(joinType_));
    if (isLeftSemiProjectJoin(joinType_) ||
        (isAntiJoin(joinType_) && filter_)) {
      // For anti join with filter and semi project join we need to decode the
      // join keys columns to initialize 'nonNullInputRows_'. The anti join
      // filter evaluation and semi project join output generation will access
      // 'nonNullInputRows_' later.
      decodeAndDetectNonNullKeys();
    }
    return;
  }

  if (!hasDecoded) {
    decodeAndDetectNonNullKeys();
  }
  activeRows_ = nonNullInputRows_;

  // Update statistics for null keys in join operator.
  // Updating here means we will report 0 null keys when build side is empty.
  // If we want more accurate stats, we will have to decode input vector
  // even when not needed. So we tradeoff less accurate stats for more
  // performance.
  {
    auto lockedStats = stats_.wlock();
    lockedStats->numNullKeys +=
        activeRows_.size() - activeRows_.countSelected();
  }

  // Remove proven misses before probing the hash table.
  applyBloomFilterForJoinProbe();

  table_->prepareForJoinProbe(*lookup_.get(), input_, activeRows_, false);

  if (joinIncludesMissesFromLeft(joinType_)) {
    // Make sure to allocate an entry in 'hits' for every input row to allow for
    // including rows without a match in the output. Also, make sure to
    // initialize all 'hits' to nullptr as HashTable::joinProbe will only
    // process activeRows_.
    auto& hits = lookup_->hits;
    hits.resize(numInput);
    std::fill(hits.data(), hits.data() + numInput, nullptr);
    if (!lookup_->rows.empty()) {
      table_->joinProbe(*lookup_);
    }

    // Update lookup_->rows to include all input rows, not just
    // activeRows_ as we need to include all rows in the output.
    auto& rows = lookup_->rows;
    rows.resize(numInput);
    std::iota(rows.begin(), rows.end(), 0);
  } else {
    if (lookup_->rows.empty()) {
      input_ = nullptr;
      return;
    }
    lookup_->hits.resize(lookup_->rows.back() + 1);
    table_->joinProbe(*lookup_);
  }

  resultIter_->reset(*lookup_);
}

void HashProbe::prepareOutput(vector_size_t size) {
  // Try to re-use memory for the output vectors that contain build-side data.
  // We expect output vectors containing probe-side data to be null (reset in
  // clearIdentityProjectedOutput). BaseVector::prepareForReuse keeps null
  // children unmodified and makes non-null (build side) children reusable.
  if (output_) {
    VectorPtr output = std::move(output_);
    BaseVector::prepareForReuse(output, size);
    output_ = std::static_pointer_cast<RowVector>(output);
  } else {
    output_ = BaseVector::create<RowVector>(outputType_, size, pool());
  }
}

namespace {
VectorPtr createConstantFalse(vector_size_t size, memory::MemoryPool* pool) {
  return std::make_shared<ConstantVector<bool>>(
      pool, size, false /*isNull*/, BOOLEAN(), false /*value*/);
}
} // namespace

void HashProbe::fillLeftSemiProjectMatchColumn(vector_size_t size) {
  if (emptyBuildSide()) {
    // Build side is empty or all rows have null join keys.
    if (nullAware_ && buildSideHasNullKeys_) {
      matchColumn() = BaseVector::createNullConstant(BOOLEAN(), size, pool());
    } else {
      matchColumn() = createConstantFalse(size, pool());
    }
  } else {
    auto flatMatch = matchColumn()->as<FlatVector<bool>>();
    flatMatch->resize(size);
    auto rawValues = flatMatch->mutableRawValues<uint64_t>();
    auto* outputTableRows = outputTableRows_->as<char*>();
    for (auto i = 0; i < size; ++i) {
      if (nullAware_) {
        // Null-aware join may produce TRUE, FALSE or NULL.
        if (filter_) {
          if (leftSemiProjectIsNull_.isValid(i)) {
            flatMatch->setNull(i, true);
          } else {
            const bool hasMatch = outputTableRows[i] != nullptr;
            bits::setBit(rawValues, i, hasMatch);
          }
        } else {
          if (!nonNullInputRows_.isValid(i)) {
            // Probe key is null.
            flatMatch->setNull(i, true);
          } else {
            // Probe key is not null.
            const bool hasMatch = outputTableRows[i] != nullptr;
            if (!hasMatch && buildSideHasNullKeys_) {
              flatMatch->setNull(i, true);
            } else {
              bits::setBit(rawValues, i, hasMatch);
            }
          }
        }
      } else {
        const bool hasMatch = outputTableRows[i] != nullptr;
        bits::setBit(rawValues, i, hasMatch);
      }
    }
  }
}

void HashProbe::fillOutput(vector_size_t size) {
  TestValue::adjust("facebook::velox::exec::HashProbe::fillOutput", this);
  prepareOutput(size);

  for (auto [in, out] : projectedInputColumns_) {
    // Load input vector if it is being split into multiple batches. It is not
    // safe to wrap unloaded LazyVector into two different dictionaries.
    ensureLoadedIfNotAtEnd(in);
    auto inputChild = input_->childAt(in);
    output_->childAt(out) = wrapChild(size, outputRowMapping_, inputChild);
  }

  if (isLeftSemiProjectJoin(joinType_)) {
    fillLeftSemiProjectMatchColumn(size);
  } else {
    extractColumns(
        table_.get(),
        folly::Range<char* const*>(outputTableRows_->as<char*>(), size),
        tableOutputProjections_,
        pool(),
        outputType_->children(),
        output_->children());
  }
}

RowVectorPtr HashProbe::getBuildSideOutput() {
  if (buildSideOutputRowContainerId_ == -1) {
    buildSideOutputRowContainerId_ = nextBuildSideRowContainerId();
    lastProbeIterator_.reset();
  }
  if (buildSideOutputRowContainerId_ >= table_->numRowContainers()) {
    return nullptr;
  }

  char** outputTableRows =
      initBuffer<char*>(outputTableRows_, outputTableRowsCapacity_, pool());
  int32_t numOut{0};
  while (numOut == 0 &&
         buildSideOutputRowContainerId_ < table_->numRowContainers()) {
    if (isRightSemiFilterJoin(joinType_)) {
      numOut = table_->listProbedRows(
          lastProbeIterator_,
          buildSideOutputRowContainerId_,
          outputTableRowsCapacity_,
          RowContainer::kUnlimited,
          outputTableRows);
    } else if (isRightSemiProjectJoin(joinType_)) {
      numOut = table_->listAllRows(
          lastProbeIterator_,
          buildSideOutputRowContainerId_,
          outputTableRowsCapacity_,
          RowContainer::kUnlimited,
          outputTableRows);

    } else {
      // Must be a right, full, or right anti join.
      numOut = table_->listNotProbedRows(
          lastProbeIterator_,
          buildSideOutputRowContainerId_,
          outputTableRowsCapacity_,
          RowContainer::kUnlimited,
          outputTableRows);
    }
    if (numOut == 0) {
      buildSideOutputRowContainerId_ = nextBuildSideRowContainerId();
      lastProbeIterator_.reset();
    }
  }
  if (numOut == 0) {
    return nullptr;
  }

  prepareOutput(numOut);

  // Populate probe-side columns of the output with nulls.
  for (auto [in, out] : projectedInputColumns_) {
    output_->childAt(out) = BaseVector::createNullConstant(
        outputType_->childAt(out), numOut, pool());
  }

  extractColumns(
      table_.get(),
      folly::Range<char**>(outputTableRows, numOut),
      tableOutputProjections_,
      pool(),
      outputType_->children(),
      output_->children());

  if (isRightSemiProjectJoin(joinType_)) {
    // Populate 'match' column.
    if (noInput_ && nullAware_) {
      // Probe side is empty. All rows should return 'match = false', even ones
      // with a null join key. (This applies to null-aware joins only.)
      matchColumn() = createConstantFalse(numOut, pool());
    } else {
      table_->rows()->extractProbedFlags(
          outputTableRows,
          numOut,
          nullAware_,
          nullAware_ && probeSideHasNullKeys_,
          matchColumn());
    }
  }

  return output_;
}

void HashProbe::clearProjectedOutput() {
  if (!output_ || output_.use_count() != 1) {
    return;
  }
  for (auto& [_, out] : projectedInputColumns_) {
    output_->childAt(out) = nullptr;
  }
}

bool HashProbe::needLastProbe() const {
  return !skipInput_ && needRightSideJoin(joinType_);
}

bool HashProbe::skipProbeOnEmptyBuild() const {
  return isInnerJoin(joinType_) || isLeftSemiFilterJoin(joinType_) ||
      isCountingLeftSemiFilterJoin(joinType_) || isRightJoin(joinType_) ||
      isRightSemiFilterJoin(joinType_) || isRightSemiProjectJoin(joinType_) ||
      isRightAntiJoin(joinType_);
}

bool HashProbe::canSpill() const {
  if (!Operator::canSpill()) {
    return false;
  }
  // Hash table caching is incompatible with spilling. When the table is
  // cached and shared across tasks, clearing it after probe would corrupt
  // the cache for subsequent tasks.
  if (joinNode_->useHashTableCache()) {
    return false;
  }
  if (operatorCtx_->task()->hasMixedExecutionGroupJoin(joinNode_.get())) {
    return operatorCtx_->driverCtx()
               ->queryConfig()
               .mixedGroupedModeHashJoinSpillEnabled() &&
        operatorCtx_->task()->concurrentSplitGroups() == 1;
  }
  return true;
}

bool HashProbe::hasMoreSpillData() const {
  VELOX_CHECK(inputSpillPartitionSet_.empty() || canSpill());
  return !inputSpillPartitionSet_.empty() || needToSpillInput();
}

bool HashProbe::needToSpillInput() const {
  VELOX_CHECK(spillInputPartitionIds_.empty() || canSpill());
  VELOX_CHECK_EQ(spillInputPartitionIds_.empty(), inputSpiller_ == nullptr);

  return !spillInputPartitionIds_.empty();
}

void HashProbe::setState(ProbeOperatorState state) {
  checkStateTransition(state);
  state_ = state;
}

void HashProbe::checkStateTransition(ProbeOperatorState state) {
  VELOX_CHECK_NE(state_, state);
  switch (state) {
    case ProbeOperatorState::kRunning:
      VELOX_CHECK(
          state_ == ProbeOperatorState::kWaitForBuild ||
          state_ == ProbeOperatorState::kWaitForPeers);
      break;
    case ProbeOperatorState::kWaitForPeers:
      [[fallthrough]];
    case ProbeOperatorState::kWaitForBuild:
      [[fallthrough]];
    case ProbeOperatorState::kFinish:
      VELOX_CHECK_EQ(state_, ProbeOperatorState::kRunning);
      break;
    default:
      VELOX_UNREACHABLE(probeOperatorStateName(state_));
      break;
  }
}

RowVectorPtr HashProbe::getOutput() {
  // Release the extra unused memory reserved for output processing.
  SCOPE_EXIT {
    pool()->release();
  };
  return getOutputInternal(/*toSpillOutput=*/false);
}

void HashProbe::processRightSemiNoFilter(bool emptyBuildSide) {
  if (emptyBuildSide) {
    input_ = nullptr;
    return;
  }

  auto* rows = table_->rows();
  const int32_t nextOffset = rows->nextOffset();
  for (const auto probeRow : lookup_->rows) {
    char* hit = lookup_->hits[probeRow];
    while (hit != nullptr) {
      // Duplicate build rows are linked by nextOffset. The first probe that
      // reaches a key marks the whole chain. If the head row is already
      // marked, all duplicates for this key have already been marked.
      if (!rows->testAndSetProbedFlag(hit)) {
        break;
      }
      if (nextOffset == 0) {
        break;
      }
      hit = *reinterpret_cast<char**>(hit + nextOffset);
    }
  }

  input_ = nullptr;
}

RowVectorPtr HashProbe::getOutputInternal(bool toSpillOutput) {
  if (isFinished()) {
    return nullptr;
  }
  VELOX_CHECK(
      isRunning() || isWaitingForPeers(),
      fmt::format("Invalid state {}", state_));

  if (!toSpillOutput) {
    // Avoid memory reservation if it is triggered by memory arbitration to
    // spill pending output.
    ensureOutputFits();
  }

  if (maybeReadSpillOutput()) {
    return output_;
  }

  clearProjectedOutput();

  if (input_ == nullptr) {
    if (hasMoreInput()) {
      return nullptr;
    }

    if (needLastProbe() && (canOutputBuildRowsInParallel_ || lastProber_)) {
      auto output = getBuildSideOutput();
      if (output != nullptr) {
        return output;
      }
    }

    // NOTE: if getOutputInternal() is called from memory arbitration to spill
    // the produced output from pending 'input_', then we should not proceed
    // with the rest of procedure, and let the next driver getOutput() call to
    // handle the probe finishing process properly.
    if (toSpillOutput) {
      VELOX_CHECK(memory::underMemoryArbitration());
      VELOX_CHECK(canReclaim());
      return nullptr;
    }

    if (sharedSpillRestore_) {
      finishSharedPartition();
      return nullptr;
    }

    if (hasMoreSpillData()) {
      prepareForSpillRestore();
      asyncWaitForHashTable();
    } else {
      if (lastProber_ && canSpill()) {
        if (operatorCtx_->task()->hasMixedExecutionGroupJoin(joinNode_.get())) {
          const bool isLastGroup = allProbeGroupFinished();
          if (isSpillInput()) {
            // Mixed group mode has triggered spilling, based on if current
            // group is the last group or not, resetting spill partitions in
            // join bridge.
            joinBridge_->probeFinished(!isLastGroup);
          } else if (isLastGroup) {
            // Mixed group mode has NOT triggered spilling, and current group is
            // the last group (unreliable best effort signal), finish probe to
            // notify build to finish. If last group signal is not present, rely
            // on task cleanup.
            joinBridge_->probeFinished();
          }
        } else {
          joinBridge_->probeFinished();
          if (table_ != nullptr) {
            table_->clear(true);
          }
        }
        wakeupPeerOperators();
      }
      setState(ProbeOperatorState::kFinish);
    }
    return nullptr;
  }

  const auto inputSize = input_->size();

  if (replacedWithDynamicFilter_) {
    addRuntimeStat(
        std::string(HashProbe::kReplacedWithDynamicFilterRows),
        RuntimeCounter(inputSize));
    auto output = Operator::fillOutput(inputSize, nullptr);
    input_ = nullptr;
    return output;
  }

  const bool isLeftSemiOrAntiJoinNoFilter = !filter_ &&
      (isLeftSemiFilterJoin(joinType_) || isLeftSemiProjectJoin(joinType_) ||
       isAntiJoin(joinType_) || isCountingJoin(joinType_));
  const bool isRightSemiFilterJoinNoFilter =
      !filter_ && isRightSemiFilterJoin(joinType_);

  const bool emptyBuildSide = (table_->numDistinct() == 0);

  if (isRightSemiFilterJoinNoFilter) {
    processRightSemiNoFilter(emptyBuildSide);
    return nullptr;
  }

  // Left semi and anti joins are always cardinality reducing, e.g. for a
  // given row of input they produce zero or 1 row of output. Therefore, if
  // there is no extra filter we can process each batch of input in one go.
  auto outputBatchSize = (isLeftSemiOrAntiJoinNoFilter || emptyBuildSide)
      ? inputSize
      : outputBatchSize_;
  outputTableRowsCapacity_ = outputBatchSize;
  if (filter_ &&
      (isLeftJoin(joinType_) || isFullJoin(joinType_) ||
       isAntiJoin(joinType_) || isLeftSemiFilterJoin(joinType_) ||
       isLeftSemiProjectJoin(joinType_))) {
    // If we need non-matching probe side row, there is a possibility that such
    // row exists at end of an input batch and being carried over in the next
    // output batch, so we need to make extra room of one row in output.
    ++outputTableRowsCapacity_;
  }
  auto mapping = initializeRowNumberMapping(
      outputRowMapping_, outputTableRowsCapacity_, pool());
  auto* outputTableRows =
      initBuffer<char*>(outputTableRows_, outputTableRowsCapacity_, pool());

  for (;;) {
    // If the task owning this operator has been cancelled, there is no point
    // to continue executing this procedure, which may be long in degenerate
    // cases. Exit the working loop and let the Driver handle exiting gracefully
    // in its own loop.
    if (operatorCtx_->task()->isCancelled()) {
      return nullptr;
    }
    int numOut = 0;

    if (emptyBuildSide) {
      // When build side is empty, anti and left joins return all probe side
      // rows, including ones with null join keys.
      std::iota(mapping.begin(), mapping.begin() + inputSize, 0);
      std::fill(outputTableRows, outputTableRows + inputSize, nullptr);
      numOut = inputSize;
    } else if (isAntiJoin(joinType_) && !filter_) {
      if (nullAware_) {
        // When build side is not empty, anti join without a filter returns
        // probe rows with no nulls in the join key and no match in the build
        // side.
        for (auto i = 0; i < inputSize; ++i) {
          if (nonNullInputRows_.isValid(i) &&
              (!activeRows_.isValid(i) || !lookup_->hits[i])) {
            mapping[numOut] = i;
            ++numOut;
          }
        }
      } else {
        for (auto i = 0; i < inputSize; ++i) {
          if (!nonNullInputRows_.isValid(i) ||
              (!activeRows_.isValid(i) || !lookup_->hits[i])) {
            mapping[numOut] = i;
            ++numOut;
          }
        }
      }
    } else if (isCountingAntiJoin(joinType_)) {
      // Counting anti-join: emit row if no match or count has reached zero.
      // Decrement count on match.
      auto* rows = table_->rows();
      for (auto i = 0; i < inputSize; ++i) {
        auto* hit = lookup_->hits[i];
        if (!activeRows_.isValid(i) || !hit || rows->count(hit) == 0) {
          mapping[numOut] = i;
          ++numOut;
        } else {
          rows->decrementCount(hit);
        }
      }
    } else if (isCountingLeftSemiFilterJoin(joinType_)) {
      // Counting semi-join: emit row if match and count > 0.
      // Decrement count on match.
      auto* rows = table_->rows();
      for (auto i = 0; i < inputSize; ++i) {
        auto* hit = lookup_->hits[i];
        if (activeRows_.isValid(i) && hit && rows->count(hit) > 0) {
          rows->decrementCount(hit);
          mapping[numOut] = i;
          ++numOut;
        }
      }
    } else {
      numOut = table_->listJoinResults(
          *resultIter_,
          joinIncludesMissesFromLeft(joinType_),
          folly::Range(mapping.data(), outputBatchSize),
          folly::Range(outputTableRows, outputBatchSize),
          preferredOutputBatchBytes_);
    }

    // We are done processing the input batch if there are no more joined rows
    // to process and the NoMatchDetector isn't carrying forward a row that
    // still needs to be written to the output.
    if (!numOut && !noMatchDetector_.hasLastMissedRow()) {
      input_ = nullptr;
      return nullptr;
    }
    VELOX_CHECK_LE(numOut, outputBatchSize);

    // Pre-load lazy input vectors within a reclaimable section so that the
    // subsequent non-reclaimable evalFilter/fillOutput does not trigger OOM.
    // If memory reclaim spills the hash table during lazy loading,
    // ensureLazyInputLoaded() spills the current input and sets input_ to
    // nullptr. We must bail out because resultIter_ now references freed
    // hash table rows.
    if (!toSpillOutput) {
      ensureLazyInputLoaded();
      if (input_ == nullptr) {
        return nullptr;
      }
      VELOX_CHECK_NOT_NULL(table_);
    }

    numOut = evalFilter(numOut);

    if (numOut == 0) {
      // The hash probe might get stuck in the output loop if the filter is
      // highly selective. This does not apply if the call is made during
      // spilling, because we cannot break out and resume when the operator is
      // undergoing spilling.
      if (!toSpillOutput && shouldYield()) {
        return nullptr;
      }
      continue;
    }

    if (needLastProbe()) {
      // Mark build-side rows that have a match on the join condition.
      table_->rows()->setProbedFlag(outputTableRows, numOut);
    }

    // Right semi and right anti joins only return the build side output when
    // the probe side is fully complete. Do not return anything here.
    if (isRightSemiFilterJoin(joinType_) || isRightSemiProjectJoin(joinType_) ||
        isRightAntiJoin(joinType_)) {
      if (resultIter_->atEnd()) {
        input_ = nullptr;
      }
      return nullptr;
    }

    fillOutput(numOut);

    if (isLeftSemiOrAntiJoinNoFilter || emptyBuildSide) {
      input_ = nullptr;
    }
    return output_;
  }
}

bool HashProbe::maybeReadSpillOutput() {
  maybeSetupSpillOutputReader();

  if (spillOutputReader_ == nullptr) {
    return false;
  }

  VELOX_DCHECK_EQ(table_->numDistinct(), 0);

  if (!spillOutputReader_->nextBatch(output_)) {
    spillOutputReader_.reset();
    return false;
  }
  return true;
}

RowVectorPtr HashProbe::createFilterInput(vector_size_t size) {
  std::vector<VectorPtr> filterColumns(filterInputType_->size());
  for (const auto& projection : filterInputProjections_) {
    if (projectedInputColumns_.find(projection.inputChannel) !=
        projectedInputColumns_.end()) {
      // If the column is projected to the output, ensure it's loaded if it's
      // lazy in case the filter only loads an incomplete subset of the rows
      // that will be output.
      ensureLoaded(projection.inputChannel);
    } else {
      // If the column isn't projected to the output, the Vector will only be
      // reused if we've broken the input batch into multiple output batches,
      // i.e. if results_ is not at the end of the iterator.
      ensureLoadedIfNotAtEnd(projection.inputChannel);
    }

    filterColumns[projection.outputChannel] = wrapChild(
        size, outputRowMapping_, input_->childAt(projection.inputChannel));
  }

  extractColumns(
      table_.get(),
      folly::Range<char* const*>(outputTableRows_->as<char*>(), size),
      filterTableProjections_,
      pool(),
      filterInputType_->children(),
      filterColumns);

  return std::make_shared<RowVector>(
      pool(), filterInputType_, nullptr, size, std::move(filterColumns));
}

void HashProbe::prepareFilterRowsForNullAwareJoin(
    const RowVector* filterInput,
    vector_size_t numRows,
    bool filterPropagateNulls) {
  VELOX_CHECK_LE(numRows, kBatchSize);
  if (filterTableInput_ == nullptr) {
    filterTableInput_ =
        BaseVector::create<RowVector>(filterInputType_, kBatchSize, pool());
  }

  // When no rows are selected, filterInput must be nullptr since
  // createFilterInput() cannot be called when outputRowMapping_ has been
  // overwritten. When rows are selected, filterInput must be valid.
  if (FOLLY_UNLIKELY(!filterInputRows_.hasSelections())) {
    VELOX_CHECK_NULL(filterInput);
    if (filterPropagateNulls) {
      nullFilterInputRows_.resizeFill(numRows, false);
    }
    return;
  }

  VELOX_CHECK_NOT_NULL(filterInput);
  if (filterPropagateNulls) {
    nullFilterInputRows_.resizeFill(numRows, false);
    auto* rawNullRows = nullFilterInputRows_.asMutableRange().bits();
    for (auto& projection : filterInputProjections_) {
      filterInputColumnDecodedVector_.decode(
          *filterInput->childAt(projection.outputChannel), filterInputRows_);
      if (filterInputColumnDecodedVector_.mayHaveNulls()) {
        if (const uint64_t* nulls =
                filterInputColumnDecodedVector_.nulls(&filterInputRows_)) {
          SelectivityVector nullsInActiveRows(numRows);
          memcpy(
              nullsInActiveRows.asMutableRange().bits(),
              nulls,
              bits::nbytes(numRows));
          // All rows that are not active count as non-null here.
          bits::orWithNegatedBits(
              nullsInActiveRows.asMutableRange().bits(),
              filterInputRows_.asRange().bits(),
              0,
              numRows);
          // NOTE: the false value of a raw null bit indicates null so we OR
          // with negative of the raw bit.
          bits::orWithNegatedBits(
              rawNullRows, nullsInActiveRows.asRange().bits(), 0, numRows);
        }
      }
    }
    nullFilterInputRows_.updateBounds();
    // TODO: consider to skip filtering on 'nullFilterInputRows_' as we know
    // it will never pass the filtering.
  }

  // NOTE: for null-aware anti join, we will skip filtering on the probe rows
  // with null join key columns(s) as we can apply filtering after they cross
  // join with the table rows later.
  if (!nonNullInputRows_.isAllSelected()) {
    auto* rawMapping = outputRowMapping_->asMutable<vector_size_t>();
    for (int i = 0; i < numRows; ++i) {
      if (filterInputRows_.isValid(i) &&
          !nonNullInputRows_.isValid(rawMapping[i])) {
        filterInputRows_.setValid(i, false);
      }
    }
    filterInputRows_.updateBounds();
  }
}

namespace {

const uint64_t* getFlatFilterResult(VectorPtr& result) {
  if (!result->isFlatEncoding()) {
    return nullptr;
  }
  auto* flat = result->asUnchecked<FlatVector<bool>>();
  if (!flat->mayHaveNulls()) {
    return flat->rawValues<uint64_t>();
  }
  if (!flat->rawValues<uint64_t>()) {
    return flat->rawNulls();
  }
  if (result.use_count() != 1) {
    return nullptr;
  }
  auto* values = flat->mutableRawValues<uint64_t>();
  bits::andBits(values, flat->rawNulls(), 0, flat->size());
  return values;
}

} // namespace

void HashProbe::applyFilterOnTableRowsForNullAwareJoin(
    SelectivityVector& rows,
    SelectivityVector& filterPassedRows,
    std::function<int32_t(char**, int32_t)> iterator) {
  if (!rows.hasSelections()) {
    return;
  }
  VELOX_CHECK(table_->rows(), "Should not move rows in hash joins");
  char* data[kBatchSize];

  while (auto numBuildRows = iterator(data, kBatchSize)) {
    // Extract build-side columns once per build batch.
    filterTableInput_->resize(numBuildRows);
    filterTableInputRows_.resizeFill(numBuildRows, true);
    for (auto& projection : filterTableProjections_) {
      table_->extractColumn(
          folly::Range<char* const*>(data, numBuildRows),
          projection.inputChannel,
          filterTableInput_->childAt(projection.outputChannel));
    }

    // Skip probe rows that already passed the filter on a previous build batch.
    rows.deselect(filterPassedRows);
    rows.applyToSelected([&](vector_size_t row) {
      for (auto& projection : filterInputProjections_) {
        filterTableInput_->childAt(projection.outputChannel) =
            BaseVector::wrapInConstant(
                numBuildRows, row, input_->childAt(projection.inputChannel));
      }
      EvalCtx evalCtx(
          operatorCtx_->execCtx(), filter_.get(), filterTableInput_.get());
      filter_->eval(filterTableInputRows_, evalCtx, filterTableResult_);

      bool passed = false;
      if (auto* values = getFlatFilterResult(filterTableResult_[0])) {
        passed = !bits::testSetBits(
            values, 0, numBuildRows, [](vector_size_t) { return false; });
      } else {
        decodedFilterTableResult_.decode(
            *filterTableResult_[0], filterTableInputRows_);
        if (decodedFilterTableResult_.isConstantMapping()) {
          passed = !decodedFilterTableResult_.isNullAt(0) &&
              decodedFilterTableResult_.valueAt<bool>(0);
        } else {
          for (vector_size_t i = 0; i < numBuildRows; ++i) {
            if (!decodedFilterTableResult_.isNullAt(i) &&
                decodedFilterTableResult_.valueAt<bool>(i)) {
              passed = true;
              break;
            }
          }
        }
      }
      if (passed) {
        filterPassedRows.setValid(row, true);
      }
    });
  }
}

SelectivityVector HashProbe::evalFilterForNullAwareJoin(
    vector_size_t numRows,
    bool filterPropagateNulls) {
  auto* rawOutputProbeRowMapping =
      outputRowMapping_->asMutable<vector_size_t>();

  // Subset of probe-side rows with a match that passed the filter.
  SelectivityVector filterPassedRows(input_->size(), false);

  // Subset of probe-side rows with non-null probe key and either no match or
  // no match that passed the filter. We need to combine these with all
  // build-side rows with null keys to see if a filter passes on any of these.
  SelectivityVector nullKeyProbeRows(input_->size(), false);

  // Subset of probe-sie rows with null probe key. We need to combine these
  // with all build-side rows to see if a filter passes on any of these.
  SelectivityVector crossJoinProbeRows(input_->size(), false);

  for (auto i = 0; i < numRows; ++i) {
    // Skip filter input row if it has any null probe side filter column.
    if (filterPropagateNulls && nullFilterInputRows_.isValid(i)) {
      continue;
    }

    const auto probeRow = rawOutputProbeRowMapping[i];
    if (nonNullInputRows_.isValid(probeRow)) {
      if (filterPassed(i)) {
        filterPassedRows.setValid(probeRow, true);
      } else {
        nullKeyProbeRows.setValid(probeRow, true);
      }
    } else {
      crossJoinProbeRows.setValid(probeRow, true);
    }
  }

  if (buildSideHasNullKeys_) {
    prepareNullKeyProbeHashers();
    BaseHashTable::NullKeyRowsIterator iter;
    nullKeyProbeRows.updateBounds();
    applyFilterOnTableRowsForNullAwareJoin(
        nullKeyProbeRows, filterPassedRows, [&](char** data, int32_t maxRows) {
          return table_->listNullKeyRows(
              &iter, maxRows, data, nullKeyProbeHashers_);
        });
  }
  BaseHashTable::RowsIterator iter;
  crossJoinProbeRows.updateBounds();
  applyFilterOnTableRowsForNullAwareJoin(
      crossJoinProbeRows, filterPassedRows, [&](char** data, int32_t maxRows) {
        return table_->listAllRows(
            &iter, maxRows, RowContainer::kUnlimited, data);
      });
  filterPassedRows.updateBounds();

  return filterPassedRows;
}

void HashProbe::prepareNullKeyProbeHashers() {
  if (nullKeyProbeHashers_.empty()) {
    nullKeyProbeHashers_ =
        createVectorHashers(probeType_, joinNode_->leftKeys());
    // Null-aware joins allow only one join key.
    VELOX_CHECK_EQ(nullKeyProbeHashers_.size(), 1);
    if (table_->hashMode() == BaseHashTable::HashMode::kHash) {
      nullKeyProbeInput_ =
          BaseVector::create(nullKeyProbeHashers_[0]->type(), 1, pool());
      nullKeyProbeInput_->setNull(0, true);
      SelectivityVector selectivity(1);
      nullKeyProbeHashers_[0]->decode(*nullKeyProbeInput_, selectivity);
    }
  }
}

int32_t HashProbe::evalFilter(int32_t numRows) {
  if (!filter_) {
    return numRows;
  }

  const bool filterPropagateNulls = filter_->expr(0)->propagatesNulls();
  auto* rawOutputProbeRowMapping =
      outputRowMapping_->asMutable<vector_size_t>();
  auto* outputTableRows = outputTableRows_->asMutable<char*>();

  filterInputRows_.resizeFill(numRows);

  // Do not evaluate filter on rows with no match to (1) avoid
  // false-positives when filter evaluates to true for rows with NULLs on the
  // build side; (2) avoid errors in filter evaluation that would fail the
  // query unnecessarily.
  // TODO Apply the same to left joins.
  if (isAntiJoin(joinType_) || isLeftSemiProjectJoin(joinType_)) {
    for (auto i = 0; i < numRows; ++i) {
      if (outputTableRows[i] == nullptr) {
        filterInputRows_.setValid(i, false);
      }
    }
    filterInputRows_.updateBounds();
  }

  // Skip filter evaluation when no rows are selected. filterPassed()
  // short-circuits on filterInputRows_, so the result is never read.
  // We cannot call createFilterInput() here because it wraps probe columns
  // in a dictionary over outputRowMapping_, which listJoinResults() may have
  // already overwritten in the previous iteration — the dictionary would fail
  // validation in debug builds.
  if (FOLLY_LIKELY(filterInputRows_.hasSelections())) {
    RowVectorPtr filterInput = createFilterInput(numRows);
    if (nullAware_) {
      prepareFilterRowsForNullAwareJoin(
          filterInput.get(), numRows, filterPropagateNulls);
    }

    EvalCtx evalCtx(operatorCtx_->execCtx(), filter_.get(), filterInput.get());
    filter_->eval(0, 1, true, filterInputRows_, evalCtx, filterResult_);

    decodedFilterResult_.decode(*filterResult_[0], filterInputRows_);
  } else if (nullAware_) {
    prepareFilterRowsForNullAwareJoin(nullptr, numRows, filterPropagateNulls);
  }

  int32_t numPassed = 0;
  if (isLeftJoin(joinType_) || isFullJoin(joinType_)) {
    // Identify probe rows which got filtered out and add them back with nulls
    // for build side.
    if (noMatchDetector_.hasLastMissedRow()) {
      auto* tempOutputTableRows = initBuffer<char*>(
          tempOutputTableRows_, outputTableRowsCapacity_, pool());
      auto* tempOutputRowMapping = initBuffer<vector_size_t>(
          tempOutputRowMapping_, outputTableRowsCapacity_, pool());
      auto addMiss = [&](auto row) {
        tempOutputTableRows[numPassed] = nullptr;
        tempOutputRowMapping[numPassed++] = row;
      };
      for (auto i = 0; i < numRows; ++i) {
        const bool passed = filterPassed(i);
        noMatchDetector_.advance(rawOutputProbeRowMapping[i], passed, addMiss);
        if (passed) {
          tempOutputTableRows[numPassed] = outputTableRows[i];
          tempOutputRowMapping[numPassed++] = rawOutputProbeRowMapping[i];
        }
      }
      if (resultIter_->atEnd()) {
        noMatchDetector_.finish(addMiss);
      }
      std::copy(
          tempOutputTableRows,
          tempOutputTableRows + numPassed,
          outputTableRows);
      std::copy(
          tempOutputRowMapping,
          tempOutputRowMapping + numPassed,
          rawOutputProbeRowMapping);
    } else {
      auto addMiss = [&](auto row) {
        outputTableRows[numPassed] = nullptr;
        rawOutputProbeRowMapping[numPassed++] = row;
      };
      for (auto i = 0; i < numRows; ++i) {
        const bool passed = filterPassed(i);
        noMatchDetector_.advance(rawOutputProbeRowMapping[i], passed, addMiss);
        if (passed) {
          outputTableRows[numPassed] = outputTableRows[i];
          rawOutputProbeRowMapping[numPassed++] = rawOutputProbeRowMapping[i];
        }
      }
      if (resultIter_->atEnd()) {
        noMatchDetector_.finish(addMiss);
      }
    }
  } else if (isLeftSemiFilterJoin(joinType_)) {
    auto addLastMatch = [&](auto row) {
      outputTableRows[numPassed] = nullptr;
      rawOutputProbeRowMapping[numPassed++] = row;
    };
    for (auto i = 0; i < numRows; ++i) {
      if (filterPassed(i)) {
        leftSemiFilterJoinTracker_.advance(
            rawOutputProbeRowMapping[i], addLastMatch);
      }
    }
    if (resultIter_->atEnd()) {
      leftSemiFilterJoinTracker_.finish(addLastMatch);
    }
  } else if (isLeftSemiProjectJoin(joinType_)) {
    // NOTE: Set output table row to point to a fake string to indicate there
    // is a match for this probe 'row'. 'fillOutput' populates the match
    // column based on the nullable of this pointer.
    static const char* kPassed = "passed";

    if (nullAware_) {
      leftSemiProjectIsNull_.resize(numRows);
      leftSemiProjectIsNull_.clearAll();

      auto addLast = [&](auto row, std::optional<bool> passed) {
        if (passed.has_value()) {
          outputTableRows[numPassed] =
              passed.value() ? const_cast<char*>(kPassed) : nullptr;
        } else {
          leftSemiProjectIsNull_.setValid(numPassed, true);
        }
        rawOutputProbeRowMapping[numPassed++] = row;
      };

      auto passedRows =
          evalFilterForNullAwareJoin(numRows, filterPropagateNulls);
      for (auto i = 0; i < numRows; ++i) {
        // filterPassed(i) -> TRUE
        // else passed -> NULL
        // else FALSE
        auto probeRow = rawOutputProbeRowMapping[i];
        std::optional<bool> passed = filterPassed(i)
            ? std::optional(true)
            : (passedRows.isValid(probeRow) ? std::nullopt
                                            : std::optional(false));
        leftSemiProjectJoinTracker_.advance(probeRow, passed, addLast);
      }
      leftSemiProjectIsNull_.updateBounds();
      if (resultIter_->atEnd()) {
        leftSemiProjectJoinTracker_.finish(addLast);
      }
    } else {
      auto addLast = [&](auto row, std::optional<bool> passed) {
        outputTableRows[numPassed] =
            passed.value() ? const_cast<char*>(kPassed) : nullptr;
        rawOutputProbeRowMapping[numPassed++] = row;
      };
      for (auto i = 0; i < numRows; ++i) {
        leftSemiProjectJoinTracker_.advance(
            rawOutputProbeRowMapping[i], filterPassed(i), addLast);
      }
      if (resultIter_->atEnd()) {
        leftSemiProjectJoinTracker_.finish(addLast);
      }
    }
  } else if (isAntiJoin(joinType_)) {
    auto addMiss = [&](auto row) {
      outputTableRows[numPassed] = nullptr;
      rawOutputProbeRowMapping[numPassed++] = row;
    };
    if (nullAware_) {
      auto passedRows =
          evalFilterForNullAwareJoin(numRows, filterPropagateNulls);
      for (auto i = 0; i < numRows; ++i) {
        auto probeRow = rawOutputProbeRowMapping[i];
        bool passed = passedRows.isValid(probeRow);
        noMatchDetector_.advance(probeRow, passed, addMiss);
      }
    } else {
      for (auto i = 0; i < numRows; ++i) {
        auto probeRow = rawOutputProbeRowMapping[i];
        noMatchDetector_.advance(probeRow, filterPassed(i), addMiss);
      }
    }
    if (resultIter_->atEnd()) {
      noMatchDetector_.finish(addMiss);
    }
  } else {
    for (auto i = 0; i < numRows; ++i) {
      if (filterPassed(i)) {
        outputTableRows[numPassed] = outputTableRows[i];
        rawOutputProbeRowMapping[numPassed++] = rawOutputProbeRowMapping[i];
      }
    }
  }
  VELOX_CHECK_LE(numPassed, outputTableRowsCapacity_);
  return numPassed;
}

void HashProbe::ensureLoadedIfNotAtEnd(column_index_t channel) {
  if (resultIter_->atEnd()) {
    return;
  }

  ensureLoaded(channel);
}

void HashProbe::ensureLoaded(column_index_t channel) {
  if (!filter_ &&
      (isLeftSemiFilterJoin(joinType_) || isLeftSemiProjectJoin(joinType_) ||
       isAntiJoin(joinType_) || isCountingJoin(joinType_))) {
    return;
  }

  if (!passingInputRowsInitialized_) {
    passingInputRowsInitialized_ = true;
    passingInputRows_.resize(input_->size());
    if (joinIncludesMissesFromLeft(joinType_)) {
      passingInputRows_.setAll();
    } else {
      passingInputRows_.clearAll();
      auto hitsSize = lookup_->hits.size();
      auto hits = lookup_->hits.data();
      for (auto i = 0; i < hitsSize; ++i) {
        if (hits[i]) {
          passingInputRows_.setValid(i, true);
        }
      }

      passingInputRows_.updateBounds();
    }
  }

  LazyVector::ensureLoadedRows(input_->childAt(channel), passingInputRows_);
}

void HashProbe::noMoreInput() {
  Operator::noMoreInput();
  noMoreInputInternal();
}

bool HashProbe::hasMoreInput() const {
  return !noMoreInput_ || (spillInputReader_ != nullptr && !noMoreSpillInput_);
}

void HashProbe::noMoreInputInternal() {
  checkRunning();

  noMoreSpillInput_ = true;
  if (!spillInputPartitionIds_.empty()) {
    VELOX_CHECK_NOT_NULL(inputSpiller_);
    VELOX_CHECK_EQ(
        spillInputPartitionIds_.size(),
        inputSpiller_->state().spilledPartitionIdSet().size());
    inputSpiller_->finishSpill(inputSpillPartitionSet_);
    VELOX_CHECK_EQ(
        spillStats_->spillSortTimeNanos.load(std::memory_order_relaxed), 0);
  }

  if (sharedSpillRestore_) {
    // No peer barrier: finishSharedPartition() releases the table once the
    // output of the pending input has been produced.
    return;
  }

  std::vector<ContinuePromise> promises;
  std::vector<std::shared_ptr<Driver>> peers;

  // Reset flags about outputting build-side rows in parallel.
  buildSideOutputRowContainerId_ = -1;

  // NOTE: if 'canSpill()' is false and 'outputBuildRowsInParallel' is
  // false too, then a hash probe operator doesn't need to wait for all the
  // other peers to finish probe processing. If 'canSpill()' is true, it
  // needs to wait and might expect spill gets triggered by the other probe
  // operators, or there is previously spilled table partition(s) that needs to
  // restore. If 'outputBuildRowsInParallel', it needs to wait to all drivers
  // start outputting build-side rows in parallel only after all drivers finish
  // probe processing.
  const bool outputBuildRowsInParallel =
      canOutputBuildRowsInParallel_ && needLastProbe();
  const bool shouldBlock = canSpill() || outputBuildRowsInParallel;
  if (!operatorCtx_->task()->allPeersFinished(
          planNodeId(),
          operatorCtx_->driver(),
          shouldBlock ? &future_ : nullptr,
          shouldBlock ? promises_ : promises,
          peers)) {
    if (shouldBlock) {
      VELOX_CHECK(future_.valid());
      setState(ProbeOperatorState::kWaitForPeers);
      VELOX_DCHECK(promises_.empty());
    } else {
      VELOX_DCHECK(promises.empty());
    }
    return;
  }

  VELOX_CHECK(promises.empty());
  lastProber_ = true;
  joinBridge_->resetUnclaimedRowContainerId();
  // If 'outputBuildRowsInParallel' is true, wake up all peers to start
  // outputting build-side rows in parallel. Otherwise, only let the last prober
  // proceed.
  if (outputBuildRowsInParallel) {
    wakeupPeerOperators();
  }
}

bool HashProbe::isFinished() {
  return state_ == ProbeOperatorState::kFinish;
}

bool HashProbe::isRunning() const {
  return state_ == ProbeOperatorState::kRunning;
}

bool HashProbe::isWaitingForPeers() const {
  return state_ == ProbeOperatorState::kWaitForPeers;
}

bool HashProbe::allProbeGroupFinished() const {
  VELOX_CHECK(
      operatorCtx_->task()->hasMixedExecutionGroupJoin(joinNode_.get()));
  VELOX_CHECK_EQ(operatorCtx_->task()->concurrentSplitGroups(), 1);
  return operatorCtx_->task()->allSplitsConsumed(joinNode_.get());
}

void HashProbe::checkRunning() const {
  VELOX_CHECK(isRunning(), probeOperatorStateName(state_));
}

void HashProbe::setRunning() {
  setState(ProbeOperatorState::kRunning);
}

bool HashProbe::nonReclaimableState() const {
  return (state_ != ProbeOperatorState::kRunning &&
          state_ != ProbeOperatorState::kWaitForPeers) ||
      nonReclaimableSection_ || (inputSpiller_ != nullptr) ||
      (table_ == nullptr) || (table_->numDistinct() == 0);
}

void HashProbe::ensureOutputFits() {
  if (!canReclaim()) {
    // Don't reserve memory if we can't reclaim from this hash probe operator.
    return;
  }

  // We only need to reserve memory for output if need.
  bool outputBuildSideRows =
      needLastProbe() && (canOutputBuildRowsInParallel_ || lastProber_);
  if (input_ == nullptr && (hasMoreInput() || !outputBuildSideRows)) {
    return;
  }

  if (testingTriggerSpill(pool()->name())) {
    Operator::ReclaimableSectionGuard guard(this);
    memory::testingRunArbitration(pool());
  }

  const uint64_t bytesToReserve = static_cast<uint64_t>(
      static_cast<double>(preferredOutputBatchBytes_) * 1.2);
  if (pool()->availableReservation() >= bytesToReserve) {
    return;
  }
  {
    Operator::ReclaimableSectionGuard guard(this);
    if (pool()->maybeReserve(bytesToReserve)) {
      return;
    }
  }
  LOG(WARNING) << "Failed to reserve " << succinctBytes(bytesToReserve)
               << " for memory pool " << pool()->name()
               << ", root pool: " << pool()->root()->name()
               << ", used: " << succinctBytes(pool()->usedBytes())
               << ", reservation: " << succinctBytes(pool()->reservedBytes())
               << ", root pool reservation: "
               << succinctBytes(pool()->root()->reservedBytes());
}

bool HashProbe::needLazyLoadProbeInput() const {
  if (!canReclaim() || input_ == nullptr || replacedWithDynamicFilter_) {
    return false;
  }

  // For left semi filter/project and anti joins without a filter,
  // ensureLoaded() is a no-op — lazy vectors are passed through zero-copy
  // via wrapChild() in fillOutput() and never loaded during probe.
  // Preloading is unnecessary because no loading happens.
  if (!filter_ &&
      (isLeftSemiFilterJoin(joinType_) || isLeftSemiProjectJoin(joinType_) ||
       isAntiJoin(joinType_))) {
    return false;
  }
  return true;
}

void HashProbe::ensureLazyInputLoaded() {
  if (!needLazyLoadProbeInput()) {
    return;
  }

  VELOX_CHECK_NOT_NULL(input_);

  // When atEnd (single output batch), fillOutput() skips loading via
  // ensureLoadedIfNotAtEnd() and wraps lazy vectors zero-copy.
  // However, createFilterInput() unconditionally loads filter+projected
  // columns, so those still need preloading when a filter is present.
  const bool atEnd{resultIter_->atEnd()};
  if (atEnd && !filter_) {
    return;
  }

  std::vector<column_index_t> lazyChannels;
  if (!atEnd) {
    for (const auto& [in, _] : projectedInputColumns_) {
      if (isLazyNotLoaded(*input_->childAt(in))) {
        lazyChannels.push_back(in);
      }
    }
  }
  if (filter_) {
    for (const auto& projection : filterInputProjections_) {
      if (atEnd &&
          projectedInputColumns_.find(projection.inputChannel) ==
              projectedInputColumns_.end()) {
        continue;
      }
      if (isLazyNotLoaded(*input_->childAt(projection.inputChannel))) {
        lazyChannels.push_back(projection.inputChannel);
      }
    }
  }
  if (lazyChannels.empty()) {
    return;
  }

  const bool tableWasNonEmpty{table_->numDistinct() > 0};
  {
    loadingLazyInput_ = true;
    SCOPE_EXIT {
      loadingLazyInput_ = false;
    };
    Operator::ReclaimableSectionGuard guard(this);
    if (testingTriggerSpill(pool()->name())) {
      memory::testingRunArbitration(pool());
    }
    for (const auto channel : lazyChannels) {
      ensureLoaded(channel);
    }
  }

  // If the hash table was spilled during lazy loading (it was non-empty
  // before but is now empty), spill the current input so it can be re-probed
  // during the restore pass. After spilling, resultIter_ references freed
  // hash table rows, so we must abandon the current iteration by setting
  // input_ to nullptr.
  VELOX_CHECK_NOT_NULL(input_);
  if (tableWasNonEmpty && table_->numDistinct() == 0) {
    VELOX_CHECK_NOT_NULL(inputSpiller_);
    spillInput(input_);
    if (input_ != nullptr) {
      // Non-spilled rows remain. For join types that skip probe on empty
      // build side (inner, right, semi filter), dropping them is correct.
      // For left/anti/full joins this would be data loss — fail loudly.
      VELOX_CHECK(skipProbeOnEmptyBuild());
    }
    input_ = nullptr;
  }
}

bool HashProbe::canReclaim() const {
  return canSpill() && !exceededMaxSpillLevelLimit_;
}

void HashProbe::reclaim(
    uint64_t /*unused*/,
    memory::MemoryReclaimer::Stats& stats) {
  TestValue::adjust("facebook::velox::exec::HashProbe::reclaim", this);
  VELOX_CHECK(canSpill());
  auto* driver = operatorCtx_->driver();
  VELOX_CHECK_NOT_NULL(driver);
  VELOX_CHECK(!nonReclaimableSection_);
  if (sharedSpillRestore_) {
    reclaimShared(stats);
    return;
  }

  if (UNLIKELY(exceededMaxSpillLevelLimit_)) {
    // 'canReclaim()' already checks the spill limit is not exceeding max, there
    // is only a small chance from the time 'canReclaim()' is checked to the
    // actual reclaim happens that the operator has spilled such that the spill
    // level exceeds max.
    const auto* config = spillConfig();
    VELOX_CHECK_NOT_NULL(config);
    LOG(WARNING)
        << "Can't reclaim from hash probe operator, exceeded maximum spill "
           "level of "
        << config->maxSpillLevel << ", " << pool()->name()
        << ", root pool: " << pool()->root()->name()
        << ", used: " << succinctBytes(pool()->usedBytes())
        << ", reservation: " << succinctBytes(pool()->reservedBytes())
        << ", root pool reservation: "
        << succinctBytes(pool()->root()->reservedBytes());
    return;
  }

  if (nonReclaimableState()) {
    RECORD_METRIC_VALUE(kMetricMemoryNonReclaimableCount);
    ++stats.numNonReclaimableAttempts;
    LOG(WARNING) << "Can't reclaim from hash probe operator, state_["
                 << ProbeOperatorState(state_) << "], nonReclaimableSection_["
                 << nonReclaimableSection_ << "], inputSpiller_["
                 << (inputSpiller_ == nullptr ? "nullptr" : "initialized")
                 << "], table_["
                 << (table_ == nullptr ? "nullptr" : "initialized")
                 << "], table_ numDistinct["
                 << (table_ == nullptr ? "nullptr"
                                       : std::to_string(table_->numDistinct()))
                 << "], " << pool()->name()
                 << ", root pool: " << pool()->root()->name()
                 << ", used: " << succinctBytes(pool()->usedBytes())
                 << ", reservation: " << succinctBytes(pool()->reservedBytes())
                 << ", node pool reservation: "
                 << succinctBytes(pool()->parent()->reservedBytes())
                 << ", root pool reservation: "
                 << succinctBytes(pool()->root()->reservedBytes());
    return;
  }

  const auto& task = driver->task();
  VELOX_CHECK(task->pauseRequested());
  const std::vector<HashProbe*> probeOps = findPeerOperators();
  bool hasMoreProbeInput{false};
  for (auto* probeOp : probeOps) {
    VELOX_CHECK_NOT_NULL(probeOp);
    VELOX_CHECK(probeOp->canSpill());
    if (probeOp->nonReclaimableState()) {
      RECORD_METRIC_VALUE(kMetricMemoryNonReclaimableCount);
      ++stats.numNonReclaimableAttempts;
      const auto* peerPool = probeOp->pool();
      LOG(WARNING) << "Can't reclaim from hash probe operator, state_["
                   << ProbeOperatorState(probeOp->state_)
                   << "], nonReclaimableSection_["
                   << probeOp->nonReclaimableSection_ << "], inputSpiller_["
                   << (probeOp->inputSpiller_ == nullptr ? "nullptr"
                                                         : "initialized")
                   << "], table_["
                   << (probeOp->table_ == nullptr ? "nullptr" : "initialized")
                   << "], table_ numDistinct["
                   << (probeOp->table_ == nullptr
                           ? "nullptr"
                           : std::to_string(probeOp->table_->numDistinct()))
                   << "], " << peerPool->name()
                   << ", root pool: " << peerPool->root()->name()
                   << ", used: " << succinctBytes(peerPool->usedBytes())
                   << ", reservation: "
                   << succinctBytes(peerPool->reservedBytes())
                   << ", node pool reservation: "
                   << succinctBytes(peerPool->parent()->reservedBytes())
                   << ", root pool reservation: "
                   << succinctBytes(peerPool->root()->reservedBytes());
      return;
    }
    hasMoreProbeInput |= !probeOp->noMoreSpillInput_;
  }

  spillOutput(probeOps);

  SpillPartitionSet spillPartitionSet;
  if (hasMoreProbeInput) {
    // Only spill hash table if any hash probe operators still has input probe
    // data, otherwise we skip this step.
    spillPartitionSet = spillHashJoinTable(
        table_,
        restoringPartitionId_,
        tableSpillHashBits_,
        joinNode_,
        spillConfig(),
        spillStats_.get());
    VELOX_CHECK(!spillPartitionSet.empty());
  }
  const auto spillPartitionIdSet = toSpillPartitionIdSet(spillPartitionSet);

  for (auto* probeOp : probeOps) {
    VELOX_CHECK_NOT_NULL(probeOp);
    probeOp->clearBuffers();
    // Setup all the probe operators to spill the rest of probe inputs if the
    // table has been spilled.
    if (!spillPartitionSet.empty()) {
      VELOX_CHECK(hasMoreProbeInput);
      probeOp->maybeSetupInputSpiller(spillPartitionIdSet);
    }
    probeOp->pool()->release();
  }

  // Clears memory resources held by the built hash table.
  table_->clear(true);

  // Sets the spilled hash table in the join bridge.
  if (!spillPartitionIdSet.empty()) {
    joinBridge_->appendSpilledHashTablePartitions(std::move(spillPartitionSet));
  }
}

void HashProbe::spillOutput(const std::vector<HashProbe*>& operators) {
  struct SpillResult {
    const std::exception_ptr error{nullptr};

    explicit SpillResult(std::exception_ptr _error)
        : error(std::move(_error)) {}
  };

  std::vector<std::shared_ptr<AsyncSource<SpillResult>>> spillTasks;
  auto* spillExecutor = spillConfig()->executor;
  for (auto* op : operators) {
    HashProbe* probeOp = static_cast<HashProbe*>(op);
    // Skip operators that are in the middle of loading lazy input vectors.
    // Their reclaim path re-enters getOutputInternal()->fillOutput(), or
    // evalFilter()->createFilterInput() which loads the same lazy vectors,
    // deadlocking on the column reader's folly::basic_once_flag.
    if (probeOp->loadingLazyInput_) {
      continue;
    }
    spillTasks.push_back(
        memory::createAsyncMemoryReclaimTask<SpillResult>([probeOp]() {
          try {
            probeOp->spillOutput();
            return std::make_unique<SpillResult>(nullptr);
          } catch (const std::exception& e) {
            LOG(ERROR) << "Spill output from hash probe pool "
                       << probeOp->pool()->name() << " failed: " << e.what();
            // The exception is captured and thrown by the caller.
            return std::make_unique<SpillResult>(std::current_exception());
          }
        }));
    if ((spillTasks.size() > 1) && (spillExecutor != nullptr)) {
      spillExecutor->add([source = spillTasks.back()]() { source->prepare(); });
    }
  }

  SCOPE_EXIT {
    for (auto& spillTask : spillTasks) {
      // We consume the result for the pending tasks. This is a cleanup in the
      // guard and must not throw. The first error is already captured before
      // this runs.
      try {
        spillTask->move();
      } catch (const std::exception&) {
      }
    }
  };

  for (auto& spillTask : spillTasks) {
    const auto result = spillTask->move();
    if (result->error) {
      std::rethrow_exception(result->error);
    }
  }
}

void HashProbe::spillOutput() {
  // Checks if there is any output to spill or not.
  if (input_ == nullptr && !needLastProbe()) {
    return;
  }
  // We spill all the outputs produced from 'input_' into a single partition.
  auto outputSpiller = std::make_unique<NoRowContainerSpiller>(
      outputType_,
      std::nullopt,
      HashBitRange{},
      spillConfig(),
      spillStats_.get());
  outputSpiller->setPartitionsSpilled({SpillPartitionId(0)});

  RowVectorPtr output{nullptr};
  for (;;) {
    output = getOutputInternal(/*toSpillOutput=*/true);
    if (output != nullptr) {
      // Ensure vector are lazy loaded before spilling.
      for (int32_t i = 0; i < output->childrenSize(); ++i) {
        output->childAt(i)->loadedVector();
      }
      outputSpiller->spill(SpillPartitionId(0), output);
      continue;
    }
    // NOTE: for right semi and right anti join types, we need to check if
    // 'input_' has been cleared or not instead of checking on output. These
    // joins only produce the output after processing all the probe inputs.
    if (input_ == nullptr) {
      break;
    }
    VELOX_CHECK(
        isRightSemiFilterJoin(joinType_) || isRightSemiProjectJoin(joinType_) ||
        isRightAntiJoin(joinType_));
    VELOX_CHECK((output == nullptr) && (input_ != nullptr));
  }
  VELOX_CHECK_LE(outputSpiller->state().spilledPartitionIdSet().size(), 1);

  VELOX_CHECK(spillOutputPartitionSet_.empty());
  outputSpiller->finishSpill(spillOutputPartitionSet_);
  VELOX_CHECK_EQ(spillOutputPartitionSet_.size(), 1);

  removeEmptyPartitions(spillOutputPartitionSet_);
}

void HashProbe::maybeSetupSpillOutputReader() {
  if (spillOutputPartitionSet_.empty()) {
    return;
  }
  VELOX_CHECK_EQ(spillOutputPartitionSet_.size(), 1);
  VELOX_CHECK_NULL(spillOutputReader_);

  spillOutputReader_ =
      spillOutputPartitionSet_.begin()->second->createUnorderedReader(
          spillConfig_->readBufferSize, pool(), spillStats_.get());
  spillOutputPartitionSet_.clear();
}

void HashProbe::checkMaxSpillLevel(
    const std::optional<SpillPartitionId>& restoredPartitionId) {
  if (!canSpill()) {
    return;
  }

  const auto* config = spillConfig();
  uint8_t startPartitionBit = config->startPartitionBit;
  if (restoredPartitionId.has_value()) {
    startPartitionBit = partitionBitOffset(
                            restoredPartitionId.value(),
                            config->startPartitionBit,
                            config->numPartitionBits) +
        config->numPartitionBits;
    // Disable spilling if exceeding the max spill level and the query might
    // run out of memory if the restored partition still can't fit in memory.
    if (config->exceedSpillLevelLimit(startPartitionBit)) {
      RECORD_METRIC_VALUE(kMetricMaxSpillLevelExceededCount);
      FB_LOG_EVERY_MS(WARNING, 1'000)
          << "Exceeded spill level limit: " << config->maxSpillLevel
          << ", and disable spilling for memory pool: " << pool()->name();
      exceededMaxSpillLevelLimit_ = true;
      spillStats_->spillMaxLevelExceededCount.fetch_add(
          1, std::memory_order_relaxed);
      return;
    }
  }
  exceededMaxSpillLevelLimit_ = false;
  tableSpillHashBits_ = HashBitRange(
      startPartitionBit, startPartitionBit + config->numPartitionBits);
}

void HashProbe::close() {
  Operator::close();

  abandonSharedWork();

  // Free up major memory usage.
  joinBridge_.reset();
  inputSpiller_.reset();
  table_.reset();
  spillInputReader_.reset();
  restoringPartitionId_.reset();
  spillOutputPartitionSet_.clear();
  spillOutputReader_.reset();
  clearBuffers();

  // Fulfill any pending promises
  if (lastProber_) {
    wakeupPeerOperators();
  }
}

int HashProbe::nextBuildSideRowContainerId() {
  if (sharedSpillRestore_) {
    // Only this operator emits the build-side rows of its table.
    return nextBuildSideRowContainerId_++;
  }
  return joinBridge_->getAndIncrementUnclaimedRowContainerId();
}

void HashProbe::beginFinalizing() {
  VELOX_CHECK(sharedSpillRestore_);
  VELOX_CHECK_NOT_NULL(table_);
  sharedFinalizing_ = true;
  lastProber_ = true;
  buildSideOutputRowContainerId_ = -1;
  nextBuildSideRowContainerId_ = 0;
  // The emitted rows depend on what every driver saw, not just this one. Both
  // flags only ever turn on, so merging them into this operator is safe for
  // its later partitions.
  const auto flags = joinBridge_->probeInputFlags();
  noInput_ = noInput_ && !flags.sawInput;
  probeSideHasNullKeys_ = probeSideHasNullKeys_ || flags.sawNullKeys;
}

void HashProbe::finishSharedPartition() {
  VELOX_CHECK(sharedSpillRestore_);
  VELOX_CHECK(hasSharedPartition_);
  if (sharedFinalizing_) {
    joinBridge_->finishFinalize(sharedPartition_);
    sharedFinalizing_ = false;
  } else {
    VELOX_CHECK(sharedWorkHeld_);
    // Probe rows spilled while probing this partition are new pending work.
    removeEmptyPartitions(inputSpillPartitionSet_);
    std::vector<SpillPartitionId> newWork;
    for (const auto& [id, partition] : inputSpillPartitionSet_) {
      if (!pendingSpillWork_.contains(id)) {
        newWork.push_back(id);
      }
    }
    pendingSpillWork_.insert(newWork.begin(), newWork.end());
    deleteSpillFiles(restoringProbeFiles_);
    restoringProbeFiles_.clear();
    const bool finalize = joinBridge_->releaseTable(
        sharedPartition_, newWork, {!noInput_, probeSideHasNullKeys_});
    sharedWorkHeld_ = false;
    if (finalize) {
      // The next getOutput() call emits the build-side rows of 'table_'.
      beginFinalizing();
      return;
    }
  }

  hasSharedPartition_ = false;
  sharedTableReserved_ = false;
  lastProber_ = false;
  inputSpiller_.reset();
  spillInputReader_.reset();
  restoringPartitionId_.reset();
  spillInputPartitionIds_.clear();
  noMoreSpillInput_ = false;
  startNextSharedPartition();
}

void HashProbe::startNextSharedPartition() {
  VELOX_CHECK(!hasSharedPartition_);
  if (!pendingSpillWork_.empty()) {
    std::vector<SpillPartitionId> candidates(
        pendingSpillWork_.begin(), pendingSpillWork_.end());
    std::sort(candidates.begin(), candidates.end());
    const auto next = joinBridge_->chooseNextWork(candidates);
    pendingSpillWork_.erase(next);
    sharedPartition_ = next;
    sharedWorkHeld_ = true;
  } else if (auto partition = joinBridge_->takeFinalizeWork()) {
    sharedPartition_ = std::move(partition.value());
    sharedFinalizing_ = true;
  } else {
    // 'table_' keeps pointing at the last table, which the bridge frees.
    setState(ProbeOperatorState::kFinish);
    return;
  }
  hasSharedPartition_ = true;
  table_.reset();
  asyncWaitForHashTable();
}

void HashProbe::abandonSharedWork() {
  if (!sharedSpillRestore_ || joinBridge_ == nullptr ||
      (!hasSharedPartition_ && pendingSpillWork_.empty())) {
    return;
  }
  std::optional<HashJoinBridge::PartitionKey> current;
  if (hasSharedPartition_) {
    current = sharedPartition_;
  }
  const std::vector<SpillPartitionId> pendingWork(
      pendingSpillWork_.begin(), pendingSpillWork_.end());
  try {
    joinBridge_->abandonProbeWork(
        current,
        sharedTableReserved_,
        sharedWorkHeld_,
        sharedFinalizing_,
        pendingWork);
  } catch (const std::exception& e) {
    // close() must not throw; a failed task has no work left to share.
    LOG(WARNING) << "Failed to release hash probe work: " << e.what();
  }
  hasSharedPartition_ = false;
  sharedTableReserved_ = false;
  sharedWorkHeld_ = false;
  sharedFinalizing_ = false;
  pendingSpillWork_.clear();
}

void HashProbe::reclaimShared(memory::MemoryReclaimer::Stats& stats) {
  const auto* config = spillConfig();
  VELOX_CHECK_NOT_NULL(config);
  VELOX_CHECK(operatorCtx_->task()->pauseRequested());
  if (joinBridge_ == nullptr) {
    return;
  }

  // Idle tables have no probe rows in flight, so they spill with their probed
  // flags and nothing else to coordinate.
  const auto numIdleTablesSpilled = joinBridge_->spillIdleTables(
      [&](const HashJoinBridge::PartitionKey& partition,
          const std::shared_ptr<BaseHashTable>& table) -> SpillPartitionSet {
        uint8_t startBit = config->startPartitionBit;
        if (partition.has_value()) {
          startBit = partitionBitOffset(
                         partition.value(),
                         config->startPartitionBit,
                         config->numPartitionBits) +
              config->numPartitionBits;
        }
        if (config->exceedSpillLevelLimit(startBit)) {
          return {};
        }
        return spillHashJoinTable(
            table,
            partition,
            HashBitRange(startBit, startBit + config->numPartitionBits),
            joinNode_,
            config,
            spillStats_.get());
      });

  // The join's memory reclaimer asks one probe operator to reclaim for all of
  // them, and this one may already be finished, so reclaim every table that
  // probe operators are attached to.
  std::map<BaseHashTable*, std::vector<HashProbe*>> tableOps;
  for (auto* peer : findPeerOperators()) {
    VELOX_CHECK_NOT_NULL(peer);
    if (peer->hasSharedPartition_ && peer->table_ != nullptr) {
      tableOps[peer->table_.get()].push_back(peer);
    }
  }
  if (tableOps.empty() && numIdleTablesSpilled == 0) {
    RECORD_METRIC_VALUE(kMetricMemoryNonReclaimableCount);
    ++stats.numNonReclaimableAttempts;
    return;
  }
  for (auto& [table, probeOps] : tableOps) {
    reclaimSharedTable(probeOps, stats);
  }
}

void HashProbe::reclaimSharedTable(
    const std::vector<HashProbe*>& probeOps,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_CHECK(!probeOps.empty());
  bool hasMoreProbeInput{false};
  bool hasFinalizer{false};
  int32_t numAttachedWork{0};
  for (auto* probeOp : probeOps) {
    if (probeOp->exceededMaxSpillLevelLimit_) {
      return;
    }
    if (probeOp->nonReclaimableState()) {
      RECORD_METRIC_VALUE(kMetricMemoryNonReclaimableCount);
      ++stats.numNonReclaimableAttempts;
      return;
    }
    hasMoreProbeInput |= !probeOp->noMoreSpillInput_;
    hasFinalizer |= probeOp->sharedFinalizing_;
    numAttachedWork += probeOp->sharedWorkHeld_ ? 1 : 0;
  }
  auto* owner = probeOps.front();
  VELOX_CHECK(owner->restoringPartitionId_ == owner->sharedPartition_);

  spillOutput(probeOps);

  // The table must survive (spilled) if a probe row still needs it: an
  // attached operator with input left, or a driver holding rows for this
  // partition that has not attached yet; or if its build-side rows are still
  // to be emitted and no attached operator has spilled them as output above.
  const bool needTable = hasMoreProbeInput ||
      joinBridge_->numPendingWork(owner->sharedPartition_) > numAttachedWork ||
      (needLastProbe() && !hasFinalizer);
  if (!needTable) {
    // Everything the table still had to produce was just spilled as output.
    owner->table_->clear(true);
    for (auto* probeOp : probeOps) {
      probeOp->pool()->release();
    }
    return;
  }

  auto spillPartitionSet = spillHashJoinTable(
      owner->table_,
      owner->restoringPartitionId_,
      owner->tableSpillHashBits_,
      joinNode_,
      owner->spillConfig(),
      owner->spillStats_.get());
  VELOX_CHECK(!spillPartitionSet.empty());
  const auto spillPartitionIdSet = toSpillPartitionIdSet(spillPartitionSet);
  for (auto* probeOp : probeOps) {
    probeOp->clearBuffers();
    probeOp->maybeSetupInputSpiller(spillPartitionIdSet);
    probeOp->pool()->release();
  }
  owner->table_->clear(true);
  joinBridge_->splitPartition(
      owner->sharedPartition_, std::move(spillPartitionSet));
}

void HashProbe::clearBuffers() {
  outputRowMapping_.reset();
  tempOutputRowMapping_.reset();
  outputTableRows_.reset();
  tempOutputTableRows_.reset();
  output_.reset();
  nonSpillInputIndicesBuffer_.reset();
  spillInputIndicesBuffers_.clear();
  if (filter_ == nullptr) {
    return;
  }
  filterResult_.clear();
  filterResult_.resize(1);
  filterTableInput_.reset();
  filterTableResult_.clear();
  filterTableResult_.resize(1);
  operatorCtx_->execCtx()->vectorPool()->clear();
  filter_->clearCache();
}

} // namespace facebook::velox::exec
