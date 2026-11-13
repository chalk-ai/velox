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

#include "velox/exec/HashJoinBridge.h"
#include "velox/common/file/FileSystems.h"
#include "velox/exec/HashBuild.h"

namespace facebook::velox::exec {
namespace {
static const char* kSpillProbedFlagColumnName = "__probedFlag";
}

RowTypePtr hashJoinTableType(
    const std::vector<core::FieldAccessTypedExprPtr>& joinKeys,
    const RowTypePtr& inputType,
    bool dropDuplicates) {
  const auto numKeys = joinKeys.size();

  std::vector<std::string> names;
  names.reserve(dropDuplicates ? numKeys : inputType->size());
  std::vector<TypePtr> types;
  types.reserve(dropDuplicates ? numKeys : inputType->size());
  std::unordered_set<uint32_t> keyChannelSet;
  keyChannelSet.reserve(inputType->size());

  for (int i = 0; i < numKeys; ++i) {
    auto& key = joinKeys[i];
    auto channel = exprToChannel(key.get(), inputType);
    keyChannelSet.insert(channel);
    names.emplace_back(inputType->nameOf(channel));
    types.emplace_back(inputType->childAt(channel));
  }

  if (dropDuplicates) {
    // For left semi and anti join with no extra filter, hash table does not
    // store dependent columns.
    return ROW(std::move(names), std::move(types));
  }

  for (auto i = 0; i < inputType->size(); ++i) {
    if (keyChannelSet.find(i) == keyChannelSet.end()) {
      names.emplace_back(inputType->nameOf(i));
      types.emplace_back(inputType->childAt(i));
    }
  }
  return ROW(std::move(names), std::move(types));
}

RowTypePtr hashJoinTableType(
    const std::shared_ptr<const core::HashJoinNode>& joinNode) {
  const auto inputType = joinNode->sources()[1]->outputType();
  return hashJoinTableType(
      joinNode->rightKeys(), inputType, joinNode->canDropDuplicates());
}

void HashJoinBridge::start() {
  std::lock_guard<std::mutex> l(mutex_);
  started_ = true;
  VELOX_CHECK_GT(numBuilders_, 0);
  if (sharedSpillRestore_) {
    // Every probe driver starts with pending work on the unspilled table,
    // which the HashBuild operators are building from their input.
    auto& root = sharedPartitions_[std::nullopt];
    root.state = SharedPartition::State::kBuilding;
    root.numPendingWork = numProbers_;
    totalPendingWork_ = numProbers_;
    buildInProgress_ = true;
  }
}

void HashJoinBridge::addProber(bool emitBuildSideRows) {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(!started_);
  sharedSpillRestore_ = true;
  emitBuildSideRows_ = emitBuildSideRows;
  ++numProbers_;
}

void HashJoinBridge::addBuilder() {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(!started_);
  ++numBuilders_;
}

void HashJoinBridge::reclaim() {
  std::unique_lock<std::mutex> l(mutex_);
  VELOX_CHECK(tableSpillFunc_ == nullptr || !probeStarted_);
  if (tableSpillFunc_ == nullptr) {
    return;
  }
  if (sharedSpillRestore_) {
    auto& root = sharedPartitions_.at(std::nullopt);
    if (root.state != SharedPartition::State::kInMemory ||
        root.table == nullptr || root.table->numDistinct() == 0) {
      return;
    }
    auto table = root.table;
    auto spillPartitionSet = tableSpillFunc_(table);
    table->clear(true);
    std::vector<SpillFiles> filesToDelete;
    splitLocked(
        std::nullopt,
        std::move(table),
        std::move(spillPartitionSet),
        filesToDelete);
    notifySharedWaiters(l, std::move(filesToDelete));
    return;
  }
  if (!buildResult_.has_value() || buildResult_->table == nullptr ||
      buildResult_->table->numDistinct() == 0) {
    return;
  }
  VELOX_CHECK(buildResult_.has_value());
  VELOX_CHECK_NOT_NULL(buildResult_->table);

  auto spillPartitionSet = tableSpillFunc_(buildResult_->table);
  const auto spillPartitionIdSet = toSpillPartitionIdSet(spillPartitionSet);
  buildResult_->table->clear(true);

  appendSpilledHashTablePartitionsLocked(std::move(spillPartitionSet));
  buildResult_->spillPartitionIds = spillPartitionIdSet;
  VELOX_CHECK(!restoringSpillPartitionId_.has_value());
}

namespace {
// Create spiller for spilling the row container from one of the sub-table from
// 'table' to parallelize the table spilling. The function spills all the rows
// from the row container and returns the spiller for the caller to collect the
// spilled partitions and stats.
std::unique_ptr<HashBuildSpiller> createSpiller(
    RowContainer* subTableRows,
    std::optional<SpillPartitionId> parentId,
    core::JoinType joinType,
    const RowTypePtr& tableType,
    const HashBitRange& hashBitRange,
    const common::SpillConfig* spillConfig,
    exec::SpillStats* stats) {
  return std::make_unique<HashBuildSpiller>(
      joinType,
      parentId,
      subTableRows,
      hashJoinTableSpillType(tableType, joinType),
      hashBitRange,
      spillConfig,
      stats);
}
} // namespace

std::vector<std::unique_ptr<HashJoinTableSpillResult>> spillHashJoinTable(
    const std::vector<HashBuildSpiller*>& spillers,
    const common::SpillConfig* spillConfig) {
  VELOX_CHECK_NOT_NULL(spillConfig);
  auto spillExecutor = spillConfig->executor;
  std::vector<std::shared_ptr<AsyncSource<HashJoinTableSpillResult>>>
      spillTasks;
  for (auto* spiller : spillers) {
    spillTasks.push_back(
        memory::createAsyncMemoryReclaimTask<HashJoinTableSpillResult>(
            [spiller]() {
              try {
                spiller->spill();
                return std::make_unique<HashJoinTableSpillResult>(spiller);
              } catch (const std::exception& e) {
                LOG(ERROR) << "Spill from hash join bridge failed: "
                           << e.what();
                // The exception is captured and thrown by the caller.
                return std::make_unique<HashJoinTableSpillResult>(
                    std::current_exception());
              }
            }));
    if ((spillTasks.size() > 1) && (spillExecutor != nullptr)) {
      spillExecutor->add([source = spillTasks.back()]() { source->prepare(); });
    }
  }

  auto syncGuard = folly::makeGuard([&]() {
    for (auto& spillTask : spillTasks) {
      // We consume the result for the pending tasks. This is a cleanup in the
      // guard and must not throw. The first error is already captured before
      // this runs.
      try {
        spillTask->move();
      } catch (const std::exception&) {
      }
    }
  });

  std::vector<std::unique_ptr<HashJoinTableSpillResult>> spillResults;
  for (auto& spillTask : spillTasks) {
    auto result = spillTask->move();
    if (result->error) {
      std::rethrow_exception(result->error);
    }
    spillResults.push_back(std::move(result));
  }
  return spillResults;
}

SpillPartitionSet spillHashJoinTable(
    std::shared_ptr<BaseHashTable> table,
    std::optional<SpillPartitionId> parentId,
    const HashBitRange& hashBitRange,
    const std::shared_ptr<const core::HashJoinNode>& joinNode,
    const common::SpillConfig* spillConfig,
    exec::SpillStats* spillStats) {
  VELOX_CHECK_NOT_NULL(table);
  VELOX_CHECK_NOT_NULL(spillConfig);
  if (table->numDistinct() == 0) {
    // Empty build side.
    return {};
  }

  std::vector<std::unique_ptr<HashBuildSpiller>> spillersHolder;
  std::vector<HashBuildSpiller*> spillers;
  const auto rowContainers = table->allRows();
  const auto tableType = hashJoinTableType(joinNode);
  for (auto* rowContainer : rowContainers) {
    if (rowContainer->numRows() == 0) {
      continue;
    }
    spillersHolder.push_back(createSpiller(
        rowContainer,
        parentId,
        joinNode->joinType(),
        tableType,
        hashBitRange,
        spillConfig,
        spillStats));
    spillers.push_back(spillersHolder.back().get());
  }
  if (spillersHolder.empty()) {
    return {};
  }

  const auto spillResults = spillHashJoinTable(spillers, spillConfig);

  SpillPartitionSet spillPartitions;
  for (const auto& spillResult : spillResults) {
    VELOX_CHECK_NULL(spillResult->error);
    spillResult->spiller->finishSpill(spillPartitions);
  }

  // Remove the spilled partitions which are empty so as we don't need to
  // trigger unnecessary spilling at hash probe side.
  removeEmptyPartitions(spillPartitions);
  return spillPartitions;
}

void HashJoinBridge::setHashTable(
    std::shared_ptr<BaseHashTable> table,
    SpillPartitionSet spillPartitionSet,
    bool hasNullKeys,
    HashJoinTableSpillFunc&& tableSpillFunc) {
  VELOX_CHECK_NOT_NULL(table, "setHashTable called with null table");
  VELOX_CHECK(table->numDistinct() == 0 || spillPartitionSet.empty());
  if (sharedSpillRestore_) {
    std::unique_lock<std::mutex> l(mutex_);
    VELOX_CHECK(started_);
    if (!probeStarted_ && !restoringSpillPartitionId_.has_value()) {
      tableSpillFunc_ = std::move(tableSpillFunc);
    }
    std::vector<SpillFiles> filesToDelete;
    setSharedHashTableLocked(
        std::move(table),
        std::move(spillPartitionSet),
        hasNullKeys,
        filesToDelete);
    notifySharedWaiters(l, std::move(filesToDelete));
    return;
  }
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(started_);
    VELOX_CHECK(!buildResult_.has_value());
    VELOX_CHECK(restoringSpillShards_.empty());
    if (!probeStarted_) {
      tableSpillFunc_ = std::move(tableSpillFunc);
    }
    const auto spillPartitionIdSet = toSpillPartitionIdSet(spillPartitionSet);
    appendSpilledHashTablePartitionsLocked(std::move(spillPartitionSet));
    buildResult_ = HashBuildResult(
        std::move(table),
        std::move(restoringSpillPartitionId_),
        spillPartitionIdSet,
        hasNullKeys);
    restoringSpillPartitionId_.reset();
    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

void HashJoinBridge::setHashTable(
    std::shared_ptr<wave::HashTableHolder> table,
    bool hasNullKeys) {
  VELOX_CHECK_NOT_NULL(table, "setHashTable called with null table");

  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(started_);
    VELOX_CHECK(!buildResult_.has_value());
    VELOX_CHECK(restoringSpillShards_.empty());
    buildResult_ = HashBuildResult(std::move(table), hasNullKeys);
    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

void HashJoinBridge::appendSpilledHashTablePartitions(
    SpillPartitionSet spillPartitionSet) {
  VELOX_CHECK(
      !spillPartitionSet.empty(), "Spilled table partitions can't be empty");
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(started_);
  VELOX_CHECK(!sharedSpillRestore_);
  VELOX_CHECK(probeStarted_);
  VELOX_CHECK(buildResult_.has_value());
  VELOX_CHECK(restoringSpillShards_.empty());
  VELOX_CHECK(!restoringSpillPartitionId_.has_value());
  appendSpilledHashTablePartitionsLocked(std::move(spillPartitionSet));
}

void HashJoinBridge::appendSpilledHashTablePartitionsLocked(
    SpillPartitionSet&& spillPartitionSet) {
  if (spillPartitionSet.empty()) {
    return;
  }
  spillPartitionSet_.insert(std::move(spillPartitionSet));
}

void HashJoinBridge::setAntiJoinHasNullKeys() {
  if (sharedSpillRestore_) {
    std::unique_lock<std::mutex> l(mutex_);
    VELOX_CHECK(started_);
    VELOX_CHECK(!restoringSpillPartitionId_.has_value());
    // A null build key makes the join return nothing: the unspilled table
    // carries 'hasNullKeys' without a table, and nothing else is restored.
    auto& root = sharedPartitions_.at(std::nullopt);
    root.state = SharedPartition::State::kInMemory;
    root.table = nullptr;
    root.hasNullKeys = true;
    buildInProgress_ = false;
    notifySharedWaiters(l, {});
    return;
  }
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(started_);
    VELOX_CHECK(!buildResult_.has_value());
    VELOX_CHECK(restoringSpillShards_.empty());

    buildResult_ = HashBuildResult{};
    restoringSpillPartitionId_.reset();
    spillPartitionSet_.clear();
    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

std::optional<HashJoinBridge::HashBuildResult> HashJoinBridge::tableOrFuture(
    ContinueFuture* future) {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(started_);
  VELOX_CHECK(!cancelled_, "Getting hash table after join is aborted");
  VELOX_CHECK(
      !buildResult_.has_value() ||
      (!restoringSpillPartitionId_.has_value() &&
       restoringSpillShards_.empty()));
  VELOX_CHECK(!sharedSpillRestore_);
  probeStarted_ = true;
  tableSpillFunc_ = nullptr;
  if (buildResult_.has_value()) {
    return buildResult_.value();
  }
  promises_.emplace_back("HashJoinBridge::tableOrFuture");
  *future = promises_.back().getSemiFuture();
  return std::nullopt;
}

void HashJoinBridge::probeFinished(bool restart) {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> l(mutex_);
    VELOX_CHECK(started_);
    VELOX_CHECK(!sharedSpillRestore_);
    VELOX_CHECK(buildResult_.has_value());
    VELOX_CHECK(
        !restoringSpillPartitionId_.has_value() &&
        restoringSpillShards_.empty());
    VELOX_CHECK_GT(numBuilders_, 0);
    probeStarted_ = false;
    VELOX_CHECK_NULL(tableSpillFunc_);

    if (restart) {
      // Applies only to mixed grouped execution mode. Finished current probe
      // group processing. For the next group, the spilled table partitions from
      // this finishing group processing will be reused. At the start of the
      // next group processing, probe input will be forced to spill based on the
      // this group's table spill partitions. Since the spilled partitions are
      // reused, hash build is not needed. Directly set 'buildResult_',
      // bypassing hash build. Hence do not notify the build operators.
      VELOX_CHECK(!spillPartitionSet_.hasNext());
      buildResult_->table->clear(true);
      buildResult_->restoredPartitionId = std::nullopt;
      buildResult_->spillPartitionIds =
          toSpillPartitionIdSet(spillPartitionSet_.spillPartitions());
      spillPartitionSet_.reset();
      return;
    }
    if (spillPartitionSet_.hasNext()) {
      // Finished probing one restored table from an unspilled partition. Wait
      // for the hash build operator to build a new table from the next spill
      // partition.
      buildResult_.reset();
      auto nextSpillPartition = spillPartitionSet_.next();
      restoringSpillPartitionId_ = nextSpillPartition.id();
      restoringSpillShards_ = nextSpillPartition.split(numBuilders_);
      VELOX_CHECK_EQ(restoringSpillShards_.size(), numBuilders_);
    } else {
      // Probe fully completed, resetting 'buildResult_' to signal build side to
      // finish.
      buildResult_.reset();
    }
    promises = std::move(promises_);
  }
  notify(std::move(promises));
}

std::optional<HashJoinBridge::SpillInput> HashJoinBridge::spillInputOrFuture(
    ContinueFuture* future) {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(started_);
  VELOX_CHECK(!cancelled_, "Getting spill input after join is aborted");
  VELOX_CHECK(!sharedSpillRestore_);
  VELOX_DCHECK(
      !restoringSpillPartitionId_.has_value() || !buildResult_.has_value());

  // If 'buildResult_' is set, then the probe side is under processing. The
  // build shall just wait.
  if (buildResult_.has_value()) {
    VELOX_CHECK(!restoringSpillPartitionId_.has_value());
    promises_.emplace_back("HashJoinBridge::spillInputOrFuture");
    *future = promises_.back().getSemiFuture();
    return std::nullopt;
  }

  // If 'restoringSpillPartitionId_' is not set after probe side is done, then
  // the join processing is all done.
  if (!restoringSpillPartitionId_.has_value()) {
    VELOX_CHECK(!spillPartitionSet_.hasNext());
    VELOX_CHECK(restoringSpillShards_.empty());
    return HashJoinBridge::SpillInput{};
  }

  VELOX_CHECK(!restoringSpillShards_.empty());
  auto spillShard = std::move(restoringSpillShards_.back());
  restoringSpillShards_.pop_back();
  return SpillInput(std::move(spillShard));
}

void deleteSpillFiles(const SpillFiles& files) {
  for (const auto& file : files) {
    try {
      filesystems::getFileSystem(file.path, nullptr)->remove(file.path);
    } catch (const std::exception& e) {
      // The task removes its spill directory when it finishes, so a failed
      // delete only keeps the file until then.
      LOG(WARNING) << "Failed to delete spill file " << file.path << ": "
                   << e.what();
    }
  }
}

void HashJoinBridge::notifySharedWaiters(
    std::unique_lock<std::mutex>& lock,
    std::vector<SpillFiles> filesToDelete) {
  auto promises = std::move(promises_);
  promises_.clear();
  lock.unlock();
  notify(std::move(promises));
  for (const auto& files : filesToDelete) {
    deleteSpillFiles(files);
  }
}

bool HashJoinBridge::allSharedWorkDoneLocked() const {
  return totalPendingWork_ == 0 && finalizeQueue_.empty() &&
      numFinalizing_ == 0;
}

bool HashJoinBridge::neededLocked(const PartitionKey& partition) const {
  PartitionKey key = partition;
  for (;;) {
    const auto it = sharedPartitions_.find(key);
    if (it != sharedPartitions_.end() && it->second.numPendingWork > 0) {
      return true;
    }
    if (!key.has_value()) {
      return false;
    }
    key = key->parentId();
  }
}

void HashJoinBridge::releaseUnneededLocked(
    const PartitionKey& partition,
    std::vector<SpillFiles>& filesToDelete) {
  auto it = sharedPartitions_.find(partition);
  if (it == sharedPartitions_.end()) {
    return;
  }
  auto& entry = it->second;
  if (entry.numPendingWork > 0) {
    return;
  }
  using State = SharedPartition::State;
  if (entry.state == State::kSplit) {
    // A split table is empty and its build files are already deleted; only
    // its children carry rows.
    const auto children = entry.children;
    if (entry.numAttached == 0) {
      sharedPartitions_.erase(it);
    }
    for (const auto& child : children) {
      releaseUnneededLocked(child, filesToDelete);
    }
    return;
  }
  if (emitBuildSideRows_) {
    // No probe row can reach this table any more, so its probed flags are
    // final. One driver emits its build-side rows, then finishFinalize()
    // releases it.
    if (!entry.finalizeAssigned) {
      entry.finalizeAssigned = true;
      finalizeQueue_.push_back(partition);
    }
    return;
  }
  // Attached operators release it when they detach; a running build releases
  // it when it completes.
  if (entry.numAttached > 0 || entry.state == State::kBuilding) {
    return;
  }
  if (entry.state == State::kQueued) {
    buildQueue_.erase(
        std::remove(buildQueue_.begin(), buildQueue_.end(), *partition),
        buildQueue_.end());
  }
  if (entry.table != nullptr) {
    entry.table->clear(true);
  }
  filesToDelete.push_back(std::move(entry.buildFiles));
  sharedPartitions_.erase(it);
}

void HashJoinBridge::onWorkDroppedLocked(
    const PartitionKey& partition,
    std::vector<SpillFiles>& filesToDelete) {
  if (!neededLocked(partition)) {
    releaseUnneededLocked(partition, filesToDelete);
    return;
  }
  const auto it = sharedPartitions_.find(partition);
  if (it == sharedPartitions_.end()) {
    return;
  }
  // Free an idle table that is still needed but can be rebuilt from its spill
  // files. A table that emits build-side rows carries probed flags, so it
  // stays in memory until finalized or spilled by memory arbitration.
  auto& entry = it->second;
  if (!emitBuildSideRows_ && entry.state == SharedPartition::State::kInMemory &&
      entry.numAttached == 0 && !entry.buildFiles.empty()) {
    entry.table->clear(true);
    entry.table.reset();
    entry.state = SharedPartition::State::kSpilled;
  }
}

void HashJoinBridge::splitLocked(
    const PartitionKey& partition,
    std::shared_ptr<BaseHashTable> table,
    SpillPartitionSet children,
    std::vector<SpillFiles>& filesToDelete) {
  VELOX_CHECK(!children.empty());
  auto& entry = sharedPartitions_.at(partition);
  VELOX_CHECK(
      entry.state == SharedPartition::State::kInMemory ||
      entry.state == SharedPartition::State::kBuilding);
  entry.state = SharedPartition::State::kSplit;
  entry.table = std::move(table);
  // The children hold every row of this table, so its files are obsolete.
  filesToDelete.push_back(std::move(entry.buildFiles));
  entry.buildFiles.clear();
  for (auto& [id, spillPartition] : children) {
    VELOX_CHECK_EQ(sharedPartitions_.count(id), 0, "{}", id.toString());
    auto& child = sharedPartitions_[id];
    child.buildFiles = spillPartition->files();
    entry.children.insert(id);
  }
  if (!neededLocked(partition)) {
    releaseUnneededLocked(partition, filesToDelete);
  }
}

void HashJoinBridge::maybeStartNextBuildLocked() {
  if (buildInProgress_) {
    return;
  }
  while (!buildQueue_.empty()) {
    const auto id = buildQueue_.front();
    buildQueue_.pop_front();
    auto it = sharedPartitions_.find(id);
    if (it == sharedPartitions_.end() ||
        it->second.state != SharedPartition::State::kQueued) {
      continue;
    }
    it->second.state = SharedPartition::State::kBuilding;
    buildInProgress_ = true;
    restoringSpillPartitionId_ = id;
    VELOX_CHECK(restoreBuilders_.empty());
    VELOX_CHECK_EQ(numActiveRestoreBuilders_, 0);
    // Builders read a copy: the files stay canonical until the partition is
    // released, so a dropped table can be built again. One shard per file lets
    // whichever builders are running share the work.
    const auto& files = it->second.buildFiles;
    VELOX_CHECK(!files.empty());
    SpillPartition spillPartition(id, files);
    restoringSpillShards_ = spillPartition.split(files.size());
    return;
  }
}

void HashJoinBridge::setSharedHashTableLocked(
    std::shared_ptr<BaseHashTable> table,
    SpillPartitionSet spillPartitionSet,
    bool hasNullKeys,
    std::vector<SpillFiles>& filesToDelete) {
  VELOX_CHECK(buildInProgress_);
  VELOX_CHECK(restoringSpillShards_.empty());
  const PartitionKey partition = restoringSpillPartitionId_;
  restoringSpillPartitionId_.reset();
  buildInProgress_ = false;
  ++buildGeneration_;
  VELOX_CHECK_EQ(numActiveRestoreBuilders_, 0);
  restoreBuilders_.clear();

  auto& entry = sharedPartitions_.at(partition);
  VELOX_CHECK(entry.state == SharedPartition::State::kBuilding);
  // Null-aware joins need this even when the table spilled.
  entry.hasNullKeys = hasNullKeys;
  if (!spillPartitionSet.empty()) {
    splitLocked(
        partition,
        std::move(table),
        std::move(spillPartitionSet),
        filesToDelete);
  } else {
    entry.state = SharedPartition::State::kInMemory;
    entry.table = std::move(table);
    // Releases the table if every operator that asked for it went away.
    if (entry.numAttached == 0) {
      onWorkDroppedLocked(partition, filesToDelete);
    }
  }
  maybeStartNextBuildLocked();
}

std::optional<HashJoinBridge::SpillInput> HashJoinBridge::claimRestoreShard(
    HashBuild* builder,
    uint64_t* generation,
    ContinueFuture* future) {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(started_);
  VELOX_CHECK(sharedSpillRestore_);
  VELOX_CHECK(!cancelled_, "Getting spill input after join is aborted");
  if (!restoringSpillShards_.empty()) {
    auto shard = std::move(restoringSpillShards_.back());
    restoringSpillShards_.pop_back();
    restoreBuilders_.push_back(builder);
    ++numActiveRestoreBuilders_;
    *generation = buildGeneration_;
    return SpillInput(std::move(shard));
  }
  // No probe operator can request another table once all work is done.
  if (!buildInProgress_ && allSharedWorkDoneLocked()) {
    return SpillInput{};
  }
  promises_.emplace_back("HashJoinBridge::claimRestoreShard");
  *future = promises_.back().getSemiFuture();
  return std::nullopt;
}

std::unique_ptr<SpillPartition> HashJoinBridge::claimMoreRestoreShard() {
  std::lock_guard<std::mutex> l(mutex_);
  if (restoringSpillShards_.empty()) {
    return nullptr;
  }
  auto shard = std::move(restoringSpillShards_.back());
  restoringSpillShards_.pop_back();
  return shard;
}

std::optional<std::vector<HashBuild*>> HashJoinBridge::finishRestoreShards() {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(buildInProgress_);
  VELOX_CHECK_GT(numActiveRestoreBuilders_, 0);
  // Shards are only added when a build starts, so none can appear after the
  // caller found none left.
  if (--numActiveRestoreBuilders_ > 0 || !restoringSpillShards_.empty()) {
    return std::nullopt;
  }
  return restoreBuilders_;
}

bool HashJoinBridge::restoreMergeDoneOrFuture(
    uint64_t generation,
    ContinueFuture* future) {
  std::lock_guard<std::mutex> l(mutex_);
  VELOX_CHECK(!cancelled_, "Waiting for hash table after join is aborted");
  if (buildGeneration_ > generation) {
    return true;
  }
  promises_.emplace_back("HashJoinBridge::restoreMergeDoneOrFuture");
  *future = promises_.back().getSemiFuture();
  return false;
}

std::optional<HashJoinBridge::HashBuildResult> HashJoinBridge::acquireTable(
    const PartitionKey& partition,
    bool reserved,
    ContinueFuture* future) {
  std::unique_lock<std::mutex> l(mutex_);
  VELOX_CHECK(started_);
  VELOX_CHECK(sharedSpillRestore_);
  VELOX_CHECK(!cancelled_, "Getting hash table after join is aborted");
  probeStarted_ = true;
  tableSpillFunc_ = nullptr;

  auto& entry = sharedPartitions_.at(partition);
  if (!reserved) {
    ++entry.numAttached;
  }
  using State = SharedPartition::State;
  switch (entry.state) {
    case State::kInMemory:
      if (entry.table == nullptr) {
        VELOX_CHECK(entry.hasNullKeys);
        return HashBuildResult{};
      }
      return HashBuildResult(entry.table, partition, {}, entry.hasNullKeys);
    case State::kSplit:
      return HashBuildResult(
          entry.table, partition, entry.children, entry.hasNullKeys);
    case State::kSpilled:
      VELOX_CHECK(partition.has_value());
      entry.state = State::kQueued;
      buildQueue_.push_back(partition.value());
      [[fallthrough]];
    case State::kQueued:
      [[fallthrough]];
    case State::kBuilding: {
      const bool wasBuilding = buildInProgress_;
      maybeStartNextBuildLocked();
      // Wake the idle HashBuild operators if a build started.
      std::vector<ContinuePromise> promises;
      if (!wasBuilding && buildInProgress_) {
        promises = std::move(promises_);
        promises_.clear();
      }
      promises_.emplace_back("HashJoinBridge::acquireTable");
      *future = promises_.back().getSemiFuture();
      l.unlock();
      notify(std::move(promises));
      return std::nullopt;
    }
  }
  VELOX_UNREACHABLE();
}

bool HashJoinBridge::releaseTable(
    const PartitionKey& partition,
    const std::vector<SpillPartitionId>& newWork,
    ProbeInputFlags flags) {
  std::unique_lock<std::mutex> l(mutex_);
  VELOX_CHECK(sharedSpillRestore_);
  probeInputFlags_.sawInput |= flags.sawInput;
  probeInputFlags_.sawNullKeys |= flags.sawNullKeys;

  auto& entry = sharedPartitions_.at(partition);
  VELOX_CHECK_GT(entry.numAttached, 0);
  VELOX_CHECK_GT(entry.numPendingWork, 0);
  --entry.numAttached;
  --entry.numPendingWork;
  --totalPendingWork_;
  // The children were created when this partition split, and this operator's
  // pending work on it kept them alive until now.
  for (const auto& id : newWork) {
    ++sharedPartitions_.at(id).numPendingWork;
    ++totalPendingWork_;
  }

  std::vector<SpillFiles> filesToDelete;
  bool finalize{false};
  if (emitBuildSideRows_ && entry.state == SharedPartition::State::kInMemory &&
      entry.table != nullptr && !entry.finalizeAssigned &&
      !neededLocked(partition)) {
    // The caller is the last user of the table; it emits the build-side rows
    // from the table it already holds.
    entry.finalizeAssigned = true;
    ++entry.numAttached;
    ++numFinalizing_;
    finalize = true;
  } else {
    onWorkDroppedLocked(partition, filesToDelete);
  }
  if (allSharedWorkDoneLocked()) {
    notifySharedWaiters(l, std::move(filesToDelete));
  } else {
    l.unlock();
    for (const auto& files : filesToDelete) {
      deleteSpillFiles(files);
    }
  }
  return finalize;
}

std::optional<HashJoinBridge::PartitionKey> HashJoinBridge::takeFinalizeWork() {
  std::lock_guard<std::mutex> l(mutex_);
  while (!finalizeQueue_.empty()) {
    auto partition = finalizeQueue_.front();
    finalizeQueue_.pop_front();
    if (sharedPartitions_.count(partition) == 0) {
      continue;
    }
    ++numFinalizing_;
    return partition;
  }
  return std::nullopt;
}

void HashJoinBridge::finishFinalize(const PartitionKey& partition) {
  std::unique_lock<std::mutex> l(mutex_);
  auto it = sharedPartitions_.find(partition);
  VELOX_CHECK(it != sharedPartitions_.end());
  auto& entry = it->second;
  VELOX_CHECK_GT(entry.numAttached, 0);
  VELOX_CHECK_GT(numFinalizing_, 0);
  --entry.numAttached;
  --numFinalizing_;
  std::vector<SpillFiles> filesToDelete;
  if (entry.state != SharedPartition::State::kSplit) {
    if (entry.table != nullptr) {
      entry.table->clear(true);
    }
    filesToDelete.push_back(std::move(entry.buildFiles));
  }
  if (entry.numAttached == 0) {
    sharedPartitions_.erase(it);
  }
  if (allSharedWorkDoneLocked()) {
    notifySharedWaiters(l, std::move(filesToDelete));
  } else {
    l.unlock();
    for (const auto& files : filesToDelete) {
      deleteSpillFiles(files);
    }
  }
}

HashJoinBridge::ProbeInputFlags HashJoinBridge::probeInputFlags() {
  std::lock_guard<std::mutex> l(mutex_);
  return probeInputFlags_;
}

SpillPartitionId HashJoinBridge::chooseNextWork(
    const std::vector<SpillPartitionId>& candidates) {
  VELOX_CHECK(!candidates.empty());
  std::lock_guard<std::mutex> l(mutex_);
  using State = SharedPartition::State;
  const auto rank = [&](const SpillPartitionId& id) {
    const auto it = sharedPartitions_.find(id);
    if (it == sharedPartitions_.end()) {
      return 4;
    }
    switch (it->second.state) {
      case State::kInMemory:
      case State::kSplit:
        return 0;
      case State::kBuilding:
        return 1;
      case State::kQueued:
        return 2;
      case State::kSpilled:
        return 3;
    }
    return 4;
  };
  auto best = candidates.front();
  auto bestRank = rank(best);
  for (const auto& id : candidates) {
    const auto idRank = rank(id);
    if (idRank < bestRank) {
      best = id;
      bestRank = idRank;
    }
  }
  return best;
}

void HashJoinBridge::abandonProbeWork(
    const std::optional<PartitionKey>& current,
    bool attached,
    bool holdsWork,
    bool finalizing,
    const std::vector<SpillPartitionId>& pendingWork) {
  std::unique_lock<std::mutex> l(mutex_);
  if (!sharedSpillRestore_ || !started_) {
    return;
  }
  std::vector<SpillFiles> filesToDelete;
  for (const auto& id : pendingWork) {
    --sharedPartitions_.at(id).numPendingWork;
    --totalPendingWork_;
  }
  if (current.has_value()) {
    auto& entry = sharedPartitions_.at(*current);
    if (attached) {
      --entry.numAttached;
    }
    if (holdsWork) {
      --entry.numPendingWork;
      --totalPendingWork_;
    }
    if (finalizing) {
      // Hand the build-side rows to another driver.
      --numFinalizing_;
      if (entry.state != SharedPartition::State::kSplit) {
        finalizeQueue_.push_back(*current);
      }
    }
    onWorkDroppedLocked(*current, filesToDelete);
  }
  for (const auto& id : pendingWork) {
    onWorkDroppedLocked(id, filesToDelete);
  }
  notifySharedWaiters(l, std::move(filesToDelete));
}

void HashJoinBridge::splitPartition(
    const PartitionKey& partition,
    SpillPartitionSet children) {
  std::unique_lock<std::mutex> l(mutex_);
  VELOX_CHECK(sharedSpillRestore_);
  auto table = sharedPartitions_.at(partition).table;
  std::vector<SpillFiles> filesToDelete;
  splitLocked(partition, std::move(table), std::move(children), filesToDelete);
  l.unlock();
  for (const auto& files : filesToDelete) {
    deleteSpillFiles(files);
  }
}

int32_t HashJoinBridge::numPendingWork(const PartitionKey& partition) {
  std::lock_guard<std::mutex> l(mutex_);
  const auto it = sharedPartitions_.find(partition);
  return it == sharedPartitions_.end() ? 0 : it->second.numPendingWork;
}

int32_t HashJoinBridge::spillIdleTables(
    const std::function<SpillPartitionSet(
        const PartitionKey&,
        const std::shared_ptr<BaseHashTable>&)>& spillFunc) {
  // Runs under memory arbitration with the task paused, so no operator touches
  // the tables while they spill.
  std::unique_lock<std::mutex> l(mutex_);
  // Before any probe attaches, reclaim() spills the unspilled table on behalf
  // of the build side.
  if (!sharedSpillRestore_ || !probeStarted_) {
    return 0;
  }
  std::vector<PartitionKey> idle;
  for (const auto& [partition, entry] : sharedPartitions_) {
    if (entry.state == SharedPartition::State::kInMemory &&
        entry.numAttached == 0 && entry.table != nullptr &&
        entry.table->numDistinct() > 0) {
      idle.push_back(partition);
    }
  }
  std::vector<SpillFiles> filesToDelete;
  int32_t numSpilled{0};
  for (const auto& partition : idle) {
    auto table = sharedPartitions_.at(partition).table;
    auto children = spillFunc(partition, table);
    if (children.empty()) {
      continue;
    }
    table->clear(true);
    splitLocked(
        partition, std::move(table), std::move(children), filesToDelete);
    ++numSpilled;
  }
  l.unlock();
  for (const auto& files : filesToDelete) {
    deleteSpillFiles(files);
  }
  return numSpilled;
}

bool HashJoinBridge::testingHasMoreSpilledPartitions() {
  std::lock_guard<std::mutex> l(mutex_);
  return spillPartitionSet_.hasNext();
}

bool isLeftNullAwareJoinWithFilter(
    core::JoinType joinType,
    bool nullAware,
    bool withFilter) {
  return (isAntiJoin(joinType) || isLeftSemiProjectJoin(joinType) ||
          isLeftSemiFilterJoin(joinType)) &&
      nullAware && withFilter;
}

bool isLeftNullAwareJoinWithFilter(
    const std::shared_ptr<const core::HashJoinNode>& joinNode) {
  return isLeftNullAwareJoinWithFilter(
      joinNode->joinType(),
      joinNode->isNullAware(),
      joinNode->filter() != nullptr);
}

uint64_t HashJoinMemoryReclaimer::reclaim(
    memory::MemoryPool* pool,
    uint64_t targetBytes,
    uint64_t maxWaitMs,
    memory::MemoryReclaimer::Stats& stats) {
  const auto prevNodeReservedMemory = pool->reservedBytes();

  // The flags to track if we have reclaimed from both build and probe operators
  // under a hash join node.
  bool hasReclaimedFromBuild{false};
  bool hasReclaimedFromProbe{false};
  pool->visitChildren([&](memory::MemoryPool* child) {
    VELOX_CHECK_EQ(child->kind(), memory::MemoryPool::Kind::kLeaf);
    const bool isBuild = isHashBuildMemoryPool(*child);
    if (isBuild) {
      if (!hasReclaimedFromBuild) {
        // We just need to reclaim from any one of the hash build operator.
        hasReclaimedFromBuild = true;
        child->reclaim(targetBytes, maxWaitMs, stats);
      }
      return !hasReclaimedFromProbe;
    }

    if (!hasReclaimedFromProbe) {
      // The same as build operator, we only need to reclaim from any one of the
      // hash probe operator. Skip one that can't act, e.g. because its driver
      // finished: probe operators no longer wait for each other, so some can
      // close while others still hold tables.
      uint64_t reclaimableBytes{0};
      if (child->reclaimer() == nullptr ||
          !child->reclaimer()->reclaimableBytes(*child, reclaimableBytes)) {
        return true;
      }
      hasReclaimedFromProbe = true;
      child->reclaim(targetBytes, maxWaitMs, stats);
    }
    return !hasReclaimedFromBuild;
  });

  auto currNodeReservedMemory = pool->reservedBytes();
  VELOX_CHECK_LE(currNodeReservedMemory, prevNodeReservedMemory);
  if (currNodeReservedMemory < prevNodeReservedMemory) {
    return prevNodeReservedMemory - currNodeReservedMemory;
  }

  auto joinBridge = joinBridge_.lock();
  if (joinBridge == nullptr) {
    return 0;
  }
  joinBridge->reclaim();
  currNodeReservedMemory = pool->reservedBytes();
  VELOX_CHECK_LE(currNodeReservedMemory, prevNodeReservedMemory);
  return prevNodeReservedMemory - currNodeReservedMemory;
}

bool isHashBuildMemoryPool(const memory::MemoryPool& pool) {
  return pool.name().ends_with("HashBuild");
}

bool isHashProbeMemoryPool(const memory::MemoryPool& pool) {
  return pool.name().ends_with("HashProbe");
}

bool needRightSideJoin(core::JoinType joinType) {
  return isRightJoin(joinType) || isFullJoin(joinType) ||
      isRightSemiFilterJoin(joinType) || isRightSemiProjectJoin(joinType) ||
      isRightAntiJoin(joinType);
}

RowTypePtr hashJoinTableSpillType(
    const RowTypePtr& tableType,
    core::JoinType joinType) {
  if (!needRightSideJoin(joinType)) {
    return tableType;
  }
  auto names = tableType->names();
  names.push_back(kSpillProbedFlagColumnName);
  auto types = tableType->children();
  types.push_back(BOOLEAN());
  return ROW(std::move(names), std::move(types));
}

bool isHashJoinTableSpillType(
    const RowTypePtr& spillType,
    core::JoinType joinType) {
  if (!needRightSideJoin(joinType)) {
    return true;
  }
  const column_index_t probedColumnChannel = spillType->size() - 1;
  if (!spillType->childAt(probedColumnChannel)->isBoolean()) {
    return false;
  }
  return spillType->nameOf(probedColumnChannel) == kSpillProbedFlagColumnName;
}
} // namespace facebook::velox::exec
