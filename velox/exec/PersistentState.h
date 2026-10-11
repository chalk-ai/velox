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

#include <folly/Hash.h>
#include <folly/Synchronized.h>
#include <folly/Utility.h>
#include <folly/container/F14Map.h>

#include "velox/common/memory/Memory.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::exec {

class BaseHashTable;

/// A join hash table built once and reused across iterations (the hash-table
/// reuse optimization).  'buildType' is the build row schema with key columns
/// first; 'numKeys' is how many leading columns are keys.  Dependent (payload)
/// columns occupy RowContainer indices [numKeys, buildType->size()).
struct HashTableEntry {
  std::shared_ptr<exec::BaseHashTable> table;
  RowTypePtr buildType;
  int32_t numKeys{0};
};

/// Holds the named state entries of a fixed point computation that survive
/// across iterations.  Entries are stored in a dedicated memory pool that
/// outlives the per-iteration sub-tasks, so the vectors and hash table remain
/// valid after the task that produced them is destroyed.
class PersistentState {
 public:
  explicit PersistentState(std::shared_ptr<memory::MemoryPool> pool)
      : pool_{std::move(pool)} {}

  memory::MemoryPool* pool() const {
    return pool_.get();
  }

  /// Registers Vector entry 'name' with its append mode (from its
  /// VectorStateDeclaration), creating it empty.  Append entries accumulate
  /// across iterations; replace entries (the default) are overwritten by each
  /// write.  A non-null 'distinctType' makes an append entry distinct: it keeps
  /// each row of that type at most once.  Call once, before any write.
  void declareVector(
      std::string_view name,
      bool append,
      const RowTypePtr& distinctType = nullptr);

  /// Replaces the Vector entry 'name' with a deep copy of 'batches' allocated
  /// in this store's pool, regardless of the entry's mode (a distinct entry
  /// keeps one copy of each row).  Empty batches are dropped.  Used to seed the
  /// initial value.  Returns the number of rows stored.
  vector_size_t setVector(
      std::string_view name,
      const std::vector<RowVectorPtr>& batches);

  /// Writes a deep copy of 'batches' (allocated in this store's pool) to entry
  /// 'name' honoring its declared mode: appends to an append entry (accumulate
  /// across iterations), replaces otherwise.  A distinct entry appends only the
  /// rows it does not already hold.  Empty batches are dropped.  Used by the
  /// framework to write each iteration's output entry.  Returns the number of
  /// rows written: for an append entry, the size of the new frontier.
  vector_size_t writeVector(
      std::string_view name,
      const std::vector<RowVectorPtr>& batches);

  /// Returns a snapshot (shared_ptr copy) of the full contents of Vector entry
  /// 'name' (an append entry's whole accumulation).  The returned batches are
  /// owned by this store and stay valid for its lifetime.  Returns an empty
  /// vector if the entry is absent.  Used to emit the fixed point's output.
  std::vector<RowVectorPtr> getVector(std::string_view name) const;

  /// Returns what an in-loop StateSource reads from entry 'name': an append
  /// entry's latest delta (the frontier written in the most recent write), or a
  /// replace entry's full contents.  Empty if absent.
  std::vector<RowVectorPtr> readVector(std::string_view name) const;

  /// Returns the total number of rows across all batches of the Vector entry
  /// 'name', or 0 if absent.
  vector_size_t numRows(std::string_view name) const;

  /// Stores the HashTable entry 'name'.  The table must be built in this
  /// store's pool() so it outlives the sub-tasks that probe it.
  void setHashTable(std::string_view name, HashTableEntry entry);

  /// Returns the HashTable entry 'name', or std::nullopt if absent.  Hash
  /// tables are written once during initialization and only read during
  /// iterations.
  std::optional<HashTableEntry> getHashTable(std::string_view name) const;

  /// Whether entry 'name' is declared here.  Used to resolve a state reference
  /// to the fixed point that declares it when one is nested inside another.
  bool hasVector(std::string_view name) const;

  bool hasHashTable(std::string_view name) const;

 private:
  // Stable pool for stored state.  Outlives per-iteration sub-tasks.
  const std::shared_ptr<memory::MemoryPool> pool_;

  // A Vector state entry.  'batches' is the full contents: for a replace entry
  // the current value, for an append entry the accumulation that forms the
  // output.  'frontier' is meaningful only for an append entry -- the rows of
  // the most recent write, the delta an in-loop StateSource reads (e.g. a
  // recursive CTE's working frontier).
  // The rows a distinct entry already holds, as a group-by hash table over all
  // of the entry's columns.  Defined in the .cpp.
  class DistinctRows;

  struct VectorEntry {
    bool append{false};
    std::vector<RowVectorPtr> batches;
    std::vector<RowVectorPtr> frontier;
    // The row type a distinct entry is distinct over; null otherwise.
    RowTypePtr distinctType;
    // Set for a distinct entry.  Its table lives in pool_, like the batches.
    std::shared_ptr<DistinctRows> distinctRows;
  };

  // Deep-copies 'batches' into pool_ for 'entry': all of their rows, or for a
  // distinct entry only rows it does not yet hold (which it then holds).
  std::vector<RowVectorPtr> copyForEntry(
      VectorEntry& entry,
      const std::vector<RowVectorPtr>& batches) const;

  // Transparent hash/equal so a std::string_view name looks an entry up without
  // materializing a std::string.
  template <typename T>
  using EntryMap = folly::F14FastMap<
      std::string,
      T,
      folly::transparent<folly::hasher<std::string_view>>,
      folly::transparent<std::equal_to<std::string_view>>>;

  folly::Synchronized<EntryMap<VectorEntry>> vectors_;

  folly::Synchronized<EntryMap<HashTableEntry>> hashTables_;
};

} // namespace facebook::velox::exec
