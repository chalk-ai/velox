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
#include "velox/exec/PersistentState.h"

#include <numeric>

#include "velox/exec/HashTable.h"
#include "velox/exec/VectorHasher.h"
#include "velox/vector/SelectivityVector.h"

namespace facebook::velox::exec {

namespace {

// Deep-copies 'source' into 'pool' so the result stays valid after the source's
// owning task (and its pool) is destroyed.
RowVectorPtr copyInto(const RowVectorPtr& source, memory::MemoryPool* pool) {
  auto target =
      BaseVector::create<RowVector>(source->type(), source->size(), pool);
  target->copy(source.get(), 0, 0, source->size());
  return target;
}

// Deep-copies the non-empty 'batches' into 'pool', dropping null/empty ones.
// A batch already allocated in 'pool' is taken as is: the copy exists only to
// outlive the pool the batch came from, which is nothing to do when that pool
// is this one (the caller captured the rows here directly).
std::vector<RowVectorPtr> copyBatches(
    const std::vector<RowVectorPtr>& batches,
    memory::MemoryPool* pool) {
  std::vector<RowVectorPtr> copies;
  copies.reserve(batches.size());
  for (const auto& batch : batches) {
    if (batch == nullptr || batch->size() == 0) {
      continue;
    }
    if (batch->pool() == pool) {
      copies.push_back(batch);
    } else {
      copies.push_back(copyInto(batch, pool));
    }
  }
  return copies;
}

vector_size_t countRows(const std::vector<RowVectorPtr>& batches) {
  vector_size_t numRows{0};
  for (const auto& batch : batches) {
    numRows += batch->size();
  }
  return numRows;
}

} // namespace

class PersistentState::DistinctRows {
 public:
  DistinctRows(const RowTypePtr& type, memory::MemoryPool* pool) {
    std::vector<column_index_t> channels(type->size());
    std::iota(channels.begin(), channels.end(), 0);
    // A group-by table: unlike a join build it keeps NULL keys, and a NULL
    // equals a NULL, which is how a set of rows compares them.
    table_ = std::make_unique<HashTable<false>>(
        createVectorHashers(type, channels),
        std::vector<Accumulator>{},
        std::vector<TypePtr>{},
        /*allowDuplicates=*/false,
        /*isJoinBuild=*/false,
        /*hasProbedFlag=*/false,
        /*hasCountFlag=*/false,
        /*minTableSizeForParallelJoinBuild=*/0,
        pool);
    lookup_ = std::make_unique<HashLookup>(table_->hashers(), pool);
  }

  // Adds the rows of 'batch' to the set and returns a copy, in 'pool', of the
  // ones it did not already hold, or nullptr if there are none.  A row repeated
  // within 'batch' counts once.
  RowVectorPtr addNew(const RowVectorPtr& batch, memory::MemoryPool* pool) {
    SelectivityVector rows(batch->size());
    table_->prepareForGroupProbe(
        *lookup_, batch, rows, BaseHashTable::kNoSpillInputStartPartitionBit);
    table_->groupProbe(
        *lookup_, BaseHashTable::kNoSpillInputStartPartitionBit);
    const auto& newRows = lookup_->newGroups;
    if (newRows.empty()) {
      return nullptr;
    }
    const auto numNew = static_cast<vector_size_t>(newRows.size());
    auto target = BaseVector::create<RowVector>(batch->type(), numNew, pool);
    if (numNew == batch->size()) {
      target->copy(batch.get(), 0, 0, numNew);
    } else {
      // 'newRows' lists the rows that started a group, in input order.
      target->copy(batch.get(), SelectivityVector(numNew), newRows.data());
    }
    return target;
  }

 private:
  std::unique_ptr<HashTable<false>> table_;
  std::unique_ptr<HashLookup> lookup_;
};

void PersistentState::declareVector(
    std::string_view name,
    bool append,
    const RowTypePtr& distinctType) {
  VELOX_CHECK(
      distinctType == nullptr || append,
      "Only an append vector entry can be distinct: {}",
      name);
  vectors_.withWLock([&](auto& map) {
    auto& entry = map[std::string{name}];
    entry.append = append;
    entry.distinctType = distinctType;
  });
}

std::vector<RowVectorPtr> PersistentState::copyForEntry(
    VectorEntry& entry,
    const std::vector<RowVectorPtr>& batches) const {
  if (entry.distinctRows == nullptr) {
    return copyBatches(batches, pool_.get());
  }
  std::vector<RowVectorPtr> copies;
  for (const auto& batch : batches) {
    if (batch == nullptr || batch->size() == 0) {
      continue;
    }
    if (auto newRows = entry.distinctRows->addNew(batch, pool_.get())) {
      copies.push_back(std::move(newRows));
    }
  }
  return copies;
}

vector_size_t PersistentState::setVector(
    std::string_view name,
    const std::vector<RowVectorPtr>& batches) {
  return vectors_.withWLock([&](auto& map) {
    auto& entry = map[std::string{name}];
    // Seeding replaces the entry, so a distinct entry starts a fresh set.
    if (entry.distinctType != nullptr) {
      entry.distinctRows =
          std::make_shared<DistinctRows>(entry.distinctType, pool_.get());
    }
    auto copies = copyForEntry(entry, batches);
    const auto numRows = countRows(copies);
    // Seed the frontier too, so an append entry's first in-loop read returns
    // the seed (e.g. a recursive CTE's base case).
    if (entry.append) {
      entry.frontier = copies;
    }
    entry.batches = std::move(copies);
    return numRows;
  });
}

vector_size_t PersistentState::writeVector(
    std::string_view name,
    const std::vector<RowVectorPtr>& batches) {
  return vectors_.withWLock([&](auto& map) {
    auto& entry = map[std::string{name}];
    auto copies = copyForEntry(entry, batches);
    const auto numRows = countRows(copies);
    if (!entry.append) {
      entry.batches = std::move(copies);
      return numRows;
    }
    // The new rows become the frontier (the delta the next in-loop read
    // returns -- empty when nothing was produced, which signals convergence)
    // and also accumulate into the full contents.
    entry.frontier = copies;
    entry.batches.insert(
        entry.batches.end(),
        std::make_move_iterator(copies.begin()),
        std::make_move_iterator(copies.end()));
    return numRows;
  });
}

std::vector<RowVectorPtr> PersistentState::getVector(
    std::string_view name) const {
  return vectors_.withRLock([&](const auto& map) -> std::vector<RowVectorPtr> {
    auto it = map.find(name);
    if (it == map.end()) {
      return {};
    }
    return it->second.batches;
  });
}

std::vector<RowVectorPtr> PersistentState::readVector(
    std::string_view name) const {
  return vectors_.withRLock([&](const auto& map) -> std::vector<RowVectorPtr> {
    auto it = map.find(name);
    if (it == map.end()) {
      return {};
    }
    return it->second.append ? it->second.frontier : it->second.batches;
  });
}

vector_size_t PersistentState::numRows(std::string_view name) const {
  return vectors_.withRLock([&](const auto& map) -> vector_size_t {
    auto it = map.find(name);
    if (it == map.end()) {
      return 0;
    }
    vector_size_t total{0};
    for (const auto& batch : it->second.batches) {
      total += batch->size();
    }
    return total;
  });
}

void PersistentState::setHashTable(
    std::string_view name,
    HashTableEntry entry) {
  hashTables_.withWLock(
      [&](auto& map) { map[std::string{name}] = std::move(entry); });
}

std::optional<HashTableEntry> PersistentState::getHashTable(
    std::string_view name) const {
  return hashTables_.withRLock(
      [&](const auto& map) -> std::optional<HashTableEntry> {
        auto it = map.find(name);
        if (it == map.end()) {
          return std::nullopt;
        }
        return it->second;
      });
}

bool PersistentState::hasVector(std::string_view name) const {
  return vectors_.withRLock(
      [&](const auto& map) { return map.count(name) > 0; });
}

bool PersistentState::hasHashTable(std::string_view name) const {
  return hashTables_.withRLock(
      [&](const auto& map) { return map.count(name) > 0; });
}

} // namespace facebook::velox::exec
