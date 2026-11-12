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

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "velox/common/base/Exceptions.h"
#include "velox/exec/ExchangeSink.h"

namespace facebook::velox::exec::test {

struct RecordingSinkState {
  explicit RecordingSinkState(int32_t numPartitions, bool failFinish = false)
      : partitionData(numPartitions), failFinish(failFinish) {}

  std::vector<std::string> snapshotData() const {
    std::lock_guard<std::mutex> lock(mutex);
    return partitionData;
  }

  std::vector<int64_t> snapshotRowGroupSizes() const {
    std::lock_guard<std::mutex> lock(mutex);
    return rowGroupSizes;
  }

  mutable std::mutex mutex;
  std::vector<std::string> partitionData;
  std::vector<int64_t> rowGroupSizes;
  const bool failFinish;
  std::atomic_bool finished{false};
  std::atomic_bool aborted{false};
};

class RecordingExchangeSink : public ExchangeSink {
 public:
  explicit RecordingExchangeSink(std::shared_ptr<RecordingSinkState> state)
      : state_(std::move(state)) {}

  void append(int32_t partition, std::string_view data) override {
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->partitionData.at(partition).append(data);
  }

  void append(int32_t partition, std::unique_ptr<folly::IOBuf> data) override {
    std::lock_guard<std::mutex> lock(state_->mutex);
    const auto* current = data.get();
    do {
      state_->rowGroupSizes.push_back(current->length());
      current = current->next();
    } while (current != data.get());
    data->coalesce();
    state_->partitionData.at(partition).append(
        reinterpret_cast<const char*>(data->data()), data->length());
  }

  CommittedExchangeOutput finish() override {
    if (state_->failFinish) {
      VELOX_FAIL("Simulated sink finish failure");
    }
    state_->finished = true;
    CommittedExchangeOutput output;
    for (int32_t partition = 0;
         partition < static_cast<int32_t>(state_->partitionData.size());
         ++partition) {
      output.locations.emplace(
          partition, fmt::format("partition:{}", partition));
    }
    return output;
  }

  void abort() override {
    state_->aborted = true;
  }

  folly::F14FastMap<std::string, int64_t> stats() const override {
    return {{"sharedSinkCount", 7}};
  }

 private:
  const std::shared_ptr<RecordingSinkState> state_;
};

} // namespace facebook::velox::exec::test
