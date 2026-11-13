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

#include <algorithm>
#include <deque>
#include <functional>
#include <map>

#include "velox/exec/HashBitRange.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/JoinBridge.h"
#include "velox/exec/MemoryReclaimer.h"
#include "velox/exec/Spill.h"

namespace facebook::velox::wave {
struct HashTableHolder;
}

namespace facebook::velox::exec {
class HashBuild;
class HashBuildSpiller;

namespace test {
class HashJoinBridgeTestHelper;
}

using HashJoinTableSpillFunc =
    std::function<SpillPartitionSet(std::shared_ptr<BaseHashTable>)>;

/// Hands over a hash table from a multi-threaded build pipeline to a
/// multi-threaded probe pipeline. This is owned by shared_ptr by all the build
/// and probe Operator instances concerned. Corresponds to the Presto concept of
/// the same name.
class HashJoinBridge : public JoinBridge {
 public:
  void start() override;

  /// Invoked by HashBuild operator ctor to add to this bridge by incrementing
  /// 'numBuilders_'. The latter is used to split the spill partition data among
  /// HashBuild operators to parallelize the restoring operation.
  void addBuilder();

  void reclaim();

  /// Invoked by the build operator to set the built hash table.
  /// 'spillPartitionSet' contains the spilled partitions while building
  /// 'table' which only applies if the disk spilling is enabled.
  /// Accepts both unique_ptr (regular joins) and shared_ptr (broadcast joins).
  void setHashTable(
      std::shared_ptr<BaseHashTable> table,
      SpillPartitionSet spillPartitionSet,
      bool hasNullKeys,
      HashJoinTableSpillFunc&& tableSpillFunc);

  void setHashTable(
      std::shared_ptr<wave::HashTableHolder> table,
      bool hasNullKeys);

  /// Invoked by the probe operator to append the spilled hash table partitions
  /// while probing. The function appends the spilled table partitions into
  /// 'spillPartitionSets_' stack. This only applies if the disk spilling is
  /// enabled.
  void appendSpilledHashTablePartitions(SpillPartitionSet spillPartitionSet);

  void setAntiJoinHasNullKeys();

  /// Represents the result of HashBuild operators. In case of an anti join, a
  /// build side entry with a null in a join key makes the join return nothing.
  /// In this case, HashBuild operators finishes early without processing all
  /// the input and without finishing building the hash table.
  struct HashBuildResult {
    HashBuildResult(
        std::shared_ptr<BaseHashTable> _table,
        std::optional<SpillPartitionId> _restoredPartitionId,
        SpillPartitionIdSet _spillPartitionIds,
        bool _hasNullKeys)
        : hasNullKeys(_hasNullKeys),
          table(std::move(_table)),
          restoredPartitionId(std::move(_restoredPartitionId)),
          spillPartitionIds(std::move(_spillPartitionIds)) {}

    HashBuildResult() : hasNullKeys(true) {}

    HashBuildResult(
        std::shared_ptr<wave::HashTableHolder> _table,
        bool _hasNullKeys)
        : hasNullKeys(_hasNullKeys), waveTable(std::move(_table)) {}

    bool hasNullKeys;
    std::shared_ptr<BaseHashTable> table;

    std::shared_ptr<wave::HashTableHolder> waveTable;

    /// Restored spill partition id associated with 'table', null if 'table' is
    /// not built from restoration.
    std::optional<SpillPartitionId> restoredPartitionId;

    /// Spilled partitions while building hash table. Since we don't support
    /// fine-grained spilling for hash table, either 'table' is empty or
    /// 'spillPartitionIds' is empty.
    SpillPartitionIdSet spillPartitionIds;
  };

  /// Invoked by HashProbe operator to get the table to probe which is built by
  /// HashBuild operators. If HashProbe operator calls this early, 'future' will
  /// be set to wait asynchronously, otherwise the built table along with
  /// optional spilling related information will be returned in HashBuildResult.
  std::optional<HashBuildResult> tableOrFuture(ContinueFuture* future);

  /// Invoked by HashProbe operator after finishes probing the built table to
  /// set one of the previously spilled partition to restore. The HashBuild
  /// operators will then build the next hash table from the selected spilled
  /// one.  If 'restart' is true, join bridge will reset the state to prepare
  /// for a new probing process. This is used in mixed grouped execution mode of
  /// a hash join (grouped probe, ungrouped build).
  void probeFinished(bool restart = false);

  /// Contains the spill input for one HashBuild operator: a shard of previously
  /// spilled partition data. 'spillPartition' is null if there is no more spill
  /// data to restore.
  struct SpillInput {
    explicit SpillInput(
        std::unique_ptr<SpillPartition> spillPartition = nullptr)
        : spillPartition(std::move(spillPartition)) {}

    std::unique_ptr<SpillPartition> spillPartition;
  };

  /// Invoked by HashBuild operator to get one of previously spilled partition
  /// shard to restore. The spilled partition to restore is set by HashProbe
  /// operator after finishes probing on the previously built hash table.
  /// If HashBuild operator calls this early, 'future' will be set to wait
  /// asynchronously. If there is no more spill data to restore, then
  /// 'spillPartition' will be set to null in the returned SpillInput.
  std::optional<SpillInput> spillInputOrFuture(ContinueFuture* future);

  bool testingHasMoreSpilledPartitions();

  /// Return the next unclaimed row container id in the current hash table for
  /// HashProbe drivers to output build-side rows.
  int getAndIncrementUnclaimedRowContainerId() {
    return unclaimedRowContainerId_.fetch_add(1);
  }

  /// Reset the next unclaimed row container id to 0.
  void resetUnclaimedRowContainerId() {
    unclaimedRowContainerId_.store(0);
  }

  // Shared spill restore.
  //
  // When probe-side spilling is enabled (outside mixed grouped execution),
  // HashProbe drivers never wait for each other. Each driver probes the
  // unspilled table with its own input, then restores the spilled partitions
  // it has probe rows for, in its own order. Drivers only wait for a table
  // that the HashBuild drivers are building; a table in memory is shared by
  // every driver that attaches to it.
  //
  // The bridge owns every table partition: its canonical build-side spill
  // files, its in-memory table while one exists, and the counts that decide
  // when the table and files are released:
  // - pending work: probe drivers that hold (or may still spill) probe rows for
  //   a partition. A partition is needed while it or an ancestor has pending
  //   work, since rows of an ancestor can still spill into it.
  // - attachments: probe drivers holding or waiting for the in-memory table.
  // An unneeded partition is finalized once: joins that emit build-side rows
  // (right, full, right semi, right anti) hand it to one driver to emit them;
  // then its table is freed and its spill files are deleted.

  /// Identifies a table partition. std::nullopt is the table built from the
  /// unspilled build input.
  using PartitionKey = std::optional<SpillPartitionId>;

  /// Invoked by HashProbe operator ctor to use shared spill restore.
  /// 'emitBuildSideRows' tells if the join emits build-side rows after probing
  /// a table. All probe operators must register before start().
  void addProber(bool emitBuildSideRows);

  /// Returns true if HashProbe operators registered with addProber().
  bool sharedSpillRestore() const {
    return sharedSpillRestore_;
  }

  /// Invoked by HashProbe operator to attach to the table of 'partition'.
  /// Returns the table if it is in memory or 'partition' has been split into
  /// child partitions (then 'spillPartitionIds' lists them and the table is
  /// empty). Otherwise schedules a build if needed, sets 'future', and keeps
  /// the attachment reserved: the caller passes 'reserved' = true when it
  /// retries after 'future' completes.
  std::optional<HashBuildResult> acquireTable(
      const PartitionKey& partition,
      bool reserved,
      ContinueFuture* future);

  /// Join-wide probe input properties needed to emit build-side rows.
  struct ProbeInputFlags {
    bool sawInput{false};
    bool sawNullKeys{false};
  };

  /// Invoked by HashProbe operator after it finished its probe rows for
  /// 'partition'. 'newWork' lists the child partitions it spilled probe rows
  /// into, which become its pending work. Returns true if the caller must emit
  /// the build-side rows of 'partition': it stays attached and calls
  /// finishFinalize() when done. Otherwise the caller is detached.
  bool releaseTable(
      const PartitionKey& partition,
      const std::vector<SpillPartitionId>& newWork,
      ProbeInputFlags flags);

  /// Takes an unneeded partition whose build-side rows nobody has emitted. The
  /// caller acquires its table, emits its build-side rows and calls
  /// finishFinalize(). If the acquired table was split, the caller calls
  /// finishFinalize() right away; the children are finalized on their own.
  std::optional<PartitionKey> takeFinalizeWork();

  /// Invoked after emitting the build-side rows of 'partition' (or finding it
  /// split). Frees its table and deletes its spill files.
  void finishFinalize(const PartitionKey& partition);

  /// Returns the probe input properties of all probe operators that released a
  /// table so far.
  ProbeInputFlags probeInputFlags();

  /// Picks the next partition to probe among 'candidates' (non-empty),
  /// preferring a table that is in memory or being built so that drivers share
  /// it instead of building it again.
  SpillPartitionId chooseNextWork(
      const std::vector<SpillPartitionId>& candidates);

  /// Invoked by HashProbe operator on close to drop whatever it still holds:
  /// its attachment to 'current' (if 'attached'), its pending work on
  /// 'current' (if 'holdsWork'), its finalization of 'current' (if
  /// 'finalizing', handed to another driver), and its pending work on
  /// 'pendingWork'.
  void abandonProbeWork(
      const std::optional<PartitionKey>& current,
      bool attached,
      bool holdsWork,
      bool finalizing,
      const std::vector<SpillPartitionId>& pendingWork);

  /// Invoked by HashProbe operator after spilling the attached in-memory table
  /// of 'partition' under memory pressure. 'children' holds the spilled child
  /// partitions which replace it.
  void splitPartition(
      const PartitionKey& partition,
      SpillPartitionSet children);

  /// Returns the number of probe drivers holding probe rows for 'partition'.
  int32_t numPendingWork(const PartitionKey& partition);

  /// Shared spill restore, for HashBuild operators. A table restored from
  /// spill is split into shards (one per spill file). HashBuild operators
  /// claim shards until none is left, so a table never waits for an operator
  /// that claimed nothing; the operator that reads the last outstanding shard
  /// merges the tables of every operator that read one.

  /// Returns a shard of the table being built and records 'builder' as one
  /// of its builders, returns a SpillInput without partition once the join is
  /// done, or sets 'future'. 'generation' receives the build the shard belongs
  /// to, for restoreMergeDoneOrFuture().
  std::optional<SpillInput> claimRestoreShard(
      HashBuild* builder,
      uint64_t* generation,
      ContinueFuture* future);

  /// Returns another unclaimed shard of the table being built, or null.
  std::unique_ptr<SpillPartition> claimMoreRestoreShard();

  /// Invoked by a HashBuild operator that read all the shards it claimed.
  /// Returns every operator that read a shard of this table if the caller read
  /// the last outstanding one; the caller then merges their tables.
  std::optional<std::vector<HashBuild*>> finishRestoreShards();

  /// Returns true once the table of build 'generation' has been set; otherwise
  /// sets 'future'.
  bool restoreMergeDoneOrFuture(uint64_t generation, ContinueFuture* future);

  /// Spills in-memory tables no probe operator is attached to, under memory
  /// arbitration. 'spillFunc' spills one table into its child partitions and
  /// returns an empty set if it can't be spilled. Returns the number of tables
  /// spilled.
  int32_t spillIdleTables(
      const std::function<SpillPartitionSet(
          const PartitionKey&,
          const std::shared_ptr<BaseHashTable>&)>& spillFunc);

 private:
  void appendSpilledHashTablePartitionsLocked(
      SpillPartitionSet&& spillPartitionSet);

  uint32_t numBuilders_{0};

  // The result of the build side. It is set by the last build operator when
  // build is done.
  std::optional<HashBuildResult> buildResult_;

  // Spill function that lets hash join bridge spill the hash table on behalf of
  // the hash build operator after the table ownership transfer, and before
  // probing.
  HashJoinTableSpillFunc tableSpillFunc_{nullptr};

  // restoringSpillPartitionXxx member variables are populated by the
  // bridge itself. When probe side finished processing, the bridge picks the
  // first partition from 'spillPartitionSets_', splits it into "even" shards
  // among the HashBuild operators and notifies these operators that they can
  // start building HashTables from these shards.

  // If not null, set to the currently restoring table spill partition id.
  std::optional<SpillPartitionId> restoringSpillPartitionId_;

  // If 'restoringSpillPartitionId_' is not null, this set to the restoring
  // spill partition data shards. Each shard is expected to have the same number
  // of spill files and will be processed by one of the HashBuild operator.
  std::vector<std::unique_ptr<SpillPartition>> restoringSpillShards_;

  // The spill partitions remaining to restore. This set is populated using
  // information provided by the HashBuild operators if spilling is enabled.
  // This set can grow if HashBuild operator cannot load full partition in
  // memory and engages in recursive spilling.
  IterableSpillPartitionSet spillPartitionSet_;

  // A flag indicating if any probe operator has poked 'this' join bridge to
  // attempt to get table. It is reset after probe side finish the (sub) table
  // processing.
  bool probeStarted_;

  // Keep track of the next row container id in a hash table that has not been
  // processed by any hash probe driver. This is used when hash probe drivers
  // output build-side rows. When drivers are allowed to output build-side rows
  // in parallel, drivers call getAndIncrementClaimedRowContainerId() to ensure
  // the row containers they process do not overlap with each other.
  std::atomic_int unclaimedRowContainerId_{0};

  // Shared spill restore state, guarded by 'mutex_'. See addProber().
  struct SharedPartition {
    enum class State {
      // Only the build-side spill files exist.
      kSpilled,
      // Waiting in 'buildQueue_'.
      kQueued,
      // HashBuild operators are building the table.
      kBuilding,
      // 'table' is in memory.
      kInMemory,
      // Replaced by the 'children' partitions; 'table' is empty.
      kSplit,
    };

    State state{State::kSpilled};
    // Canonical build-side spill files. Empty for the unspilled table and once
    // split.
    SpillFiles buildFiles;
    std::shared_ptr<BaseHashTable> table;
    bool hasNullKeys{false};
    SpillPartitionIdSet children;
    int32_t numPendingWork{0};
    int32_t numAttached{0};
    // Set once the build-side rows are queued for emission or being emitted.
    bool finalizeAssigned{false};
  };

  bool allSharedWorkDoneLocked() const;

  bool neededLocked(const PartitionKey& partition) const;

  // Handles 'partition' and its descendants that became unneeded.
  void releaseUnneededLocked(
      const PartitionKey& partition,
      std::vector<SpillFiles>& filesToDelete);

  void onWorkDroppedLocked(
      const PartitionKey& partition,
      std::vector<SpillFiles>& filesToDelete);

  void splitLocked(
      const PartitionKey& partition,
      std::shared_ptr<BaseHashTable> table,
      SpillPartitionSet children,
      std::vector<SpillFiles>& filesToDelete);

  void maybeStartNextBuildLocked();

  void setSharedHashTableLocked(
      std::shared_ptr<BaseHashTable> table,
      SpillPartitionSet spillPartitionSet,
      bool hasNullKeys,
      std::vector<SpillFiles>& filesToDelete);

  // Wakes every waiter in 'promises_' and deletes 'filesToDelete' outside the
  // lock.
  void notifySharedWaiters(
      std::unique_lock<std::mutex>& lock,
      std::vector<SpillFiles> filesToDelete);

  bool sharedSpillRestore_{false};
  bool emitBuildSideRows_{false};
  int32_t numProbers_{0};
  std::map<PartitionKey, SharedPartition> sharedPartitions_;
  std::deque<SpillPartitionId> buildQueue_;
  std::deque<PartitionKey> finalizeQueue_;
  // True from the start of a table build until setHashTable().
  bool buildInProgress_{false};
  // Incremented by every setHashTable().
  uint64_t buildGeneration_{0};
  // HashBuild operators that claimed a shard of the table being built.
  std::vector<HashBuild*> restoreBuilders_;
  // Operators in 'restoreBuilders_' still reading their shards.
  int32_t numActiveRestoreBuilders_{0};
  int64_t totalPendingWork_{0};
  int32_t numFinalizing_{0};
  ProbeInputFlags probeInputFlags_;

  friend test::HashJoinBridgeTestHelper;
};

// Indicates if join properties describe a null-aware anti or left semi join
// with an extra filter.
bool isLeftNullAwareJoinWithFilter(
    core::JoinType joinType,
    bool nullAware,
    bool withFilter);

// Indicates if 'joinNode' is null-aware anti or left semi project join type and
// has filter set.
bool isLeftNullAwareJoinWithFilter(
    const std::shared_ptr<const core::HashJoinNode>& joinNode);

class HashJoinMemoryReclaimer final : public MemoryReclaimer {
 public:
  static std::unique_ptr<memory::MemoryReclaimer> create(
      std::shared_ptr<HashJoinBridge> joinBridge,
      int32_t priority = 0) {
    return std::unique_ptr<memory::MemoryReclaimer>(
        new HashJoinMemoryReclaimer(joinBridge, priority));
  }

  uint64_t reclaim(
      memory::MemoryPool* pool,
      uint64_t targetBytes,
      uint64_t maxWaitMs,
      memory::MemoryReclaimer::Stats& stats) final;

 private:
  HashJoinMemoryReclaimer(
      const std::shared_ptr<HashJoinBridge>& joinBridge,
      int32_t priority)
      : MemoryReclaimer(priority), joinBridge_(joinBridge) {}
  std::weak_ptr<HashJoinBridge> joinBridge_;
};

/// Returns true if 'pool' is a hash build operator's memory pool. The check is
/// currently based on the pool name.
bool isHashBuildMemoryPool(const memory::MemoryPool& pool);

/// Returns true if 'pool' is a hash probe operator's memory pool. The check is
/// currently based on the pool name.
bool isHashProbeMemoryPool(const memory::MemoryPool& pool);

bool needRightSideJoin(core::JoinType joinType);

/// Returns the hash table row type for the specified right-side join keys and
/// input. If 'dropDuplicates' is true, keeps only key columns; otherwise
/// appends dependent non-key columns after the keys.
RowTypePtr hashJoinTableType(
    const std::vector<core::FieldAccessTypedExprPtr>& joinKeys,
    const RowTypePtr& inputType,
    bool dropDuplicates);

/// Returns the type of the hash table associated with this join.
RowTypePtr hashJoinTableType(
    const std::shared_ptr<const core::HashJoinNode>& joinNode);

struct HashJoinTableSpillResult {
  HashBuildSpiller* spiller{nullptr};
  const std::exception_ptr error{nullptr};

  explicit HashJoinTableSpillResult(std::exception_ptr _error)
      : error(_error) {}
  explicit HashJoinTableSpillResult(HashBuildSpiller* _spiller)
      : spiller(_spiller) {}
};

/// Invoked to spill the hash table from a set of spillers. If 'spillExecutor'
/// is provided, then we do parallel spill. This is used by hash build to spill
/// a partially built hash join table.
std::vector<std::unique_ptr<HashJoinTableSpillResult>> spillHashJoinTable(
    const std::vector<HashBuildSpiller*>& spillers,
    const common::SpillConfig* spillConfig);

/// Invoked to spill 'table' and returns spilled partitions. This is used by
/// hash probe or hash join bridge to spill a fully built table.
SpillPartitionSet spillHashJoinTable(
    std::shared_ptr<BaseHashTable> table,
    std::optional<SpillPartitionId> parentId,
    const HashBitRange& hashBitRange,
    const std::shared_ptr<const core::HashJoinNode>& joinNode,
    const common::SpillConfig* spillConfig,
    exec::SpillStats* spillStats);

/// Returns the type used to spill a given hash table type. The function
/// might attach a boolean column at the end of 'tableType' if 'joinType' needs
/// right side join processing. It is used by the hash join table spilling
/// triggered at the probe side to record if each row has been probed or not.
RowTypePtr hashJoinTableSpillType(
    const RowTypePtr& tableType,
    core::JoinType joinType);

/// Checks if a given type is a hash table spill type or not based on
/// 'joinType'.
bool isHashJoinTableSpillType(
    const RowTypePtr& spillType,
    core::JoinType joinType);

/// Deletes 'files' from their file system. Failures are logged: the task
/// removes its spill directory when it finishes.
void deleteSpillFiles(const SpillFiles& files);
} // namespace facebook::velox::exec
