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

#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>

#include <fmt/format.h>
#include <folly/ScopeGuard.h>
#include <folly/executors/ManualExecutor.h>
#include <folly/hash/Checksum.h>
#include <folly/testing/TestUtil.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/file/tests/FaultyFileSystem.h"
#include "velox/common/memory/Memory.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/exec/FileExchangeFormat.h"
#include "velox/exec/FileExchangeSink.h"
#include "velox/exec/FileExchangeSource.h"
#include "velox/exec/InMemoryExchangeClient.h"

namespace facebook::velox::exec {
namespace {

class FileExchangeTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    memory::MemoryManager::testingSetInstance({});
    filesystems::registerLocalFileSystem();
    tests::utils::registerFaultyFileSystem();
  }

  void SetUp() override {
    temporaryDirectory_ =
        std::make_unique<folly::test::TemporaryDirectory>("file_exchange_test");
    rootDirectory_ = temporaryDirectory_->path().string();
    rootPool_ = memory::memoryManager()->addRootPool("file_exchange_test");
    pool_ = rootPool_->addLeafChild("reader");
    previousFactories_ = std::move(ExchangeSource::factories());
    ExchangeSource::factories() = {FileExchangeSourceFactory::toFactory()};
  }

  void TearDown() override {
    tests::utils::faultyFileSystem()->clearFileFaultInjections();
    ExchangeSource::factories() = std::move(previousFactories_);
  }

  static void runUntilIdle(folly::ManualExecutor& executor) {
    for (int i = 0; i < 1'000; ++i) {
      if (executor.run() == 0) {
        return;
      }
    }
    FAIL() << "Exchange callbacks did not become idle";
  }

  static std::string pageData(const SerializedPageBase& page) {
    auto data = page.getIOBuf();
    data->coalesce();
    return std::string(
        reinterpret_cast<const char*>(data->data()), data->length());
  }

  static std::string fileContents(
      std::initializer_list<std::string_view> pages) {
    std::string contents;
    for (const auto page : pages) {
      const auto encodedPageSize = file_exchange::encodePageSize(page.size());
      contents.append(
          reinterpret_cast<const char*>(&encodedPageSize),
          sizeof(file_exchange::PageSize));
      contents.append(page);
    }
    return contents;
  }

  static uint32_t checksum(std::string_view contents) {
    return folly::crc32c(
        reinterpret_cast<const uint8_t*>(contents.data()), contents.size());
  }

  static std::string fileLocation(
      std::string_view path,
      std::string_view contents) {
    const file_exchange::ExchangeOutputFile outputFile{
        .path = std::string{path},
        .size = contents.size(),
        .checksum = checksum(contents),
    };
    return outputFile.serialize();
  }

  std::unique_ptr<folly::test::TemporaryDirectory> temporaryDirectory_;
  std::string rootDirectory_;
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> pool_;
  std::vector<ExchangeSource::Factory> previousFactories_;
};

TEST_F(FileExchangeTest, multiplePagesShareOnePartitionFile) {
  FileExchangeSink sink(rootDirectory_, "exchange", "task", 1);
  sink.append(0, "first page");
  sink.append(0, "second page");
  const auto output = sink.finish();

  const auto expectedPath =
      fmt::format("{}/exchange/partition_0/task.bin", rootDirectory_);
  const auto expectedContents = fileContents({"first page", "second page"});
  const auto outputFile =
      file_exchange::ExchangeOutputFile::deserialize(output.locations.at(0));
  EXPECT_EQ(outputFile.path, expectedPath);
  EXPECT_EQ(outputFile.size, expectedContents.size());
  EXPECT_EQ(outputFile.checksum, checksum(expectedContents));
  ASSERT_TRUE(std::filesystem::exists(expectedPath));

  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  {
    std::lock_guard<std::mutex> lock(queue->mutex());
    queue->addSourceLocked();
  }
  queue->noMoreSources();

  FileExchangeSource source(output.locations.at(0), 0, queue, pool_.get());
  const auto response =
      source.request(std::numeric_limits<uint32_t>::max(), {}).get();
  EXPECT_TRUE(response.atEnd);

  bool atEnd = false;
  ContinueFuture future;
  auto stalePromise = ContinuePromise::makeEmpty();
  std::vector<std::unique_ptr<SerializedPageBase>> pages;
  {
    std::lock_guard<std::mutex> lock(queue->mutex());
    pages = queue->dequeueLocked(
        0,
        std::numeric_limits<uint32_t>::max(),
        &atEnd,
        &future,
        &stalePromise);
  }

  ASSERT_TRUE(atEnd);
  ASSERT_EQ(pages.size(), 2);
  EXPECT_EQ(pageData(*pages[0]), "first page");
  EXPECT_EQ(pageData(*pages[1]), "second page");
}

TEST_F(FileExchangeTest, sizeProbesDoNotReadPayloads) {
  FileExchangeSink sink(rootDirectory_, "exchange", "task", 1);
  const std::string payload(4'096, 'x');
  sink.append(0, payload);
  sink.append(0, payload);
  const auto output = sink.finish();

  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  FileExchangeSource source(output.locations.at(0), 0, queue, pool_.get());
  for (int i = 0; i < 3; ++i) {
    const auto response = source.requestDataSizes({}).get();
    EXPECT_EQ(response.bytes, 0);
    EXPECT_FALSE(response.atEnd);
    EXPECT_THAT(response.remainingBytes, testing::ElementsAre(payload.size()));
    std::lock_guard<std::mutex> lock(queue->mutex());
    EXPECT_EQ(queue->totalBytes(), 0);
    EXPECT_EQ(queue->receivedPages(), 0);
  }
}

TEST_F(FileExchangeTest, dataRequestsRespectPageBudget) {
  FileExchangeSink sink(rootDirectory_, "exchange", "task", 1);
  sink.append(0, std::string(4'096, 'a'));
  sink.append(0, std::string(8'192, 'b'));
  const auto output = sink.finish();
  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  {
    std::lock_guard<std::mutex> lock(queue->mutex());
    queue->addSourceLocked();
  }
  queue->noMoreSources();
  FileExchangeSource source(output.locations.at(0), 0, queue, pool_.get());
  const auto response = source.request(6'144, {}).get();
  EXPECT_EQ(response.bytes, 4'096);
  EXPECT_FALSE(response.atEnd);
  EXPECT_THAT(response.remainingBytes, testing::ElementsAre(8'192));
  // An indivisible first page must still make progress when over budget.
  const auto oversized = source.request(1, {}).get();
  EXPECT_EQ(oversized.bytes, 8'192);
  EXPECT_TRUE(oversized.atEnd);
  EXPECT_THAT(oversized.remainingBytes, testing::IsEmpty());
}

TEST_F(FileExchangeTest, emptyPageIsRejectedBySizeProbe) {
  const auto filePath = fmt::format("{}/empty-page.bin", rootDirectory_);
  const auto contents = fileContents({""});
  std::ofstream file(filePath, std::ios::binary);
  file.write(contents.data(), contents.size());
  file.close();
  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  FileExchangeSource source(
      fileLocation(filePath, contents), 0, queue, pool_.get());
  // The exchange protocol requires strictly positive advertised page sizes.
  VELOX_ASSERT_THROW(
      source.requestDataSizes({}).get(), "Empty page payload in file");
}

TEST_F(FileExchangeTest, emptyPagesAreRejectedBeforeWriting) {
  for (int inputKind = 0; inputKind < 3; ++inputKind) {
    SCOPED_TRACE(inputKind);
    const auto exchangeId = fmt::format("exchange_{}", inputKind);
    FileExchangeSink sink(rootDirectory_, exchangeId, "task", 1);
    const auto appendEmptyPage = [&]() {
      if (inputKind == 0) {
        sink.append(0, std::string_view{});
      } else {
        auto page = folly::IOBuf::create(0);
        if (inputKind == 2) {
          page->appendToChain(folly::IOBuf::create(0));
        }
        sink.append(0, std::move(page));
      }
    };
    VELOX_ASSERT_THROW(
        appendEmptyPage(), "File exchange page must not be empty");
    EXPECT_EQ(sink.stats().at("totalPages"), 0);
    EXPECT_EQ(sink.stats().at("totalBytesWritten"), 0);
    EXPECT_FALSE(
        std::filesystem::exists(
            fmt::format(
                "{}/{}/partition_0/task.bin", rootDirectory_, exchangeId)));

    sink.append(0, "valid page");
    const auto output = sink.finish();
    const auto outputFile =
        file_exchange::ExchangeOutputFile::deserialize(output.locations.at(0));
    const auto expected = fileContents({"valid page"});
    EXPECT_EQ(outputFile.size, expected.size());
    EXPECT_EQ(outputFile.checksum, checksum(expected));
  }
}

TEST_F(FileExchangeTest, emptyFileEndsOnSizeProbe) {
  const auto filePath = fmt::format("{}/empty.bin", rootDirectory_);
  std::ofstream(filePath).close();
  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  {
    std::lock_guard<std::mutex> lock(queue->mutex());
    queue->addSourceLocked();
  }
  queue->noMoreSources();
  FileExchangeSource source(fileLocation(filePath, ""), 0, queue, pool_.get());
  // Repeated terminal probes must not enqueue duplicate end markers.
  for (int i = 0; i < 2; ++i) {
    const auto response = source.requestDataSizes({}).get();
    EXPECT_EQ(response.bytes, 0);
    EXPECT_TRUE(response.atEnd);
    EXPECT_THAT(response.remainingBytes, testing::IsEmpty());
  }
  std::lock_guard<std::mutex> lock(queue->mutex());
  EXPECT_FALSE(source.shouldRequestLocked());
  EXPECT_EQ(queue->receivedPages(), 0);
}

TEST_F(FileExchangeTest, stalledConsumerBoundsFileExchangeQueue) {
  // Exercise the actual client/source protocol with enough input to overflow
  // the queue if a metadata request accidentally transfers a page.
  constexpr int64_t kPageBytes = 4'096;
  constexpr int64_t kQueueBytes = 4 * kPageBytes;
  constexpr int kNumPages = 32;
  for (const int numSources : {1, 3}) {
    SCOPED_TRACE(numSources);
    auto leafPool = rootPool_->addLeafChild("exchange");
    folly::ManualExecutor executor;
    auto client = std::make_shared<InMemoryExchangeClient>(
        "consumer", 0, kQueueBytes, 1, 0, leafPool.get(), &executor);
    SCOPE_EXIT {
      client->close();
      runUntilIdle(executor);
    };
    for (int source = 0; source < numSources; ++source) {
      FileExchangeSink sink(
          rootDirectory_,
          fmt::format("exchange_{}", numSources),
          fmt::format("task_{}", source),
          1);
      for (int page = 0; page < kNumPages; ++page) {
        sink.append(0, std::string(kPageBytes, 'a' + source));
      }
      client->addRemoteTaskId(sink.finish().locations.at(0));
    }
    client->noMoreRemoteTasks();
    runUntilIdle(executor);
    {
      std::lock_guard<std::mutex> lock(client->queue()->mutex());
      EXPECT_GT(client->queue()->totalBytes(), 0);
      EXPECT_LE(client->queue()->totalBytes(), kQueueBytes);
      EXPECT_LE(client->queue()->peakBytes(), kQueueBytes);
      EXPECT_EQ(leafPool->usedBytes(), client->queue()->totalBytes());
    }

    int numPages = 0;
    bool atEnd = false;
    for (int i = 0; !atEnd && i < numSources * kNumPages + 2; ++i) {
      ContinueFuture future;
      auto pages = client->next(0, kPageBytes, &atEnd, &future);
      for (const auto& page : pages) {
        EXPECT_EQ(page->size(), kPageBytes);
        ++numPages;
      }
      runUntilIdle(executor);
      if (pages.empty() && !atEnd && !future.isReady()) {
        FAIL() << "Exchange made no progress";
      }
    }
    EXPECT_TRUE(atEnd);
    EXPECT_EQ(numPages, numSources * kNumPages);
    client->close();
    runUntilIdle(executor);
    EXPECT_EQ(leafPool->usedBytes(), 0);
  }
}

TEST_F(FileExchangeTest, oversizedPagesMakeProgress) {
  constexpr int64_t kPageBytes = 32'768;
  constexpr int64_t kQueueBytes = kPageBytes / 2;
  constexpr int kNumPages = 3;
  for (const bool skipSizeProbe : {false, true}) {
    SCOPED_TRACE(skipSizeProbe);
    folly::ManualExecutor executor;
    auto client = std::make_shared<InMemoryExchangeClient>(
        "consumer",
        0,
        kQueueBytes,
        1,
        24'576,
        pool_.get(),
        &executor,
        10,
        skipSizeProbe,
        true);
    SCOPE_EXIT {
      client->close();
      runUntilIdle(executor);
    };
    FileExchangeSink sink(
        rootDirectory_, fmt::format("exchange_{}", skipSizeProbe), "task", 1);
    for (int i = 0; i < kNumPages; ++i) {
      sink.append(0, std::string(kPageBytes, 'a' + i));
    }
    client->addRemoteTaskId(sink.finish().locations.at(0));
    client->noMoreRemoteTasks();

    bool atEnd = false;
    int numPages = 0;
    for (int i = 0; !atEnd && i < kNumPages + 2; ++i) {
      ContinueFuture future;
      auto pages = client->next(0, 1, &atEnd, &future);
      for (const auto& page : pages) {
        EXPECT_EQ(pageData(*page), std::string(kPageBytes, 'a' + numPages));
        ++numPages;
      }
      runUntilIdle(executor);
      std::lock_guard<std::mutex> lock(client->queue()->mutex());
      // The progress allowance admits one indivisible page, then stalls.
      EXPECT_LE(client->queue()->totalBytes(), kPageBytes);
      EXPECT_LE(client->queue()->peakBytes(), kPageBytes);
    }
    EXPECT_TRUE(atEnd);
    EXPECT_EQ(numPages, kNumPages);
  }
}

TEST_F(FileExchangeTest, clientCloseReleasesQueuedPayloads) {
  constexpr int64_t kPageBytes = 4'096;
  FileExchangeSink sink(rootDirectory_, "exchange", "task", 1);
  sink.append(0, std::string(kPageBytes, 'x'));
  sink.append(0, std::string(kPageBytes, 'y'));
  const auto output = sink.finish();
  folly::ManualExecutor executor;
  auto client = std::make_shared<InMemoryExchangeClient>(
      "consumer", 0, kPageBytes, 1, 0, pool_.get(), &executor);
  client->addRemoteTaskId(output.locations.at(0));
  client->noMoreRemoteTasks();
  // Complete the initial metadata callback, leaving the data callback pending.
  ASSERT_EQ(executor.step(), 1);
  EXPECT_EQ(pool_->usedBytes(), kPageBytes);
  client->close();
  EXPECT_EQ(pool_->usedBytes(), 0);
  runUntilIdle(executor);
  EXPECT_EQ(pool_->usedBytes(), 0);
}

TEST_F(FileExchangeTest, checksumFailureReleasesQueuedPayloads) {
  constexpr int64_t kPageBytes = 4'096;
  FileExchangeSink sink(rootDirectory_, "exchange", "task", 1);
  sink.append(0, std::string(kPageBytes, 'x'));
  sink.append(0, std::string(kPageBytes, 'y'));
  const auto output = sink.finish();
  const auto outputFile =
      file_exchange::ExchangeOutputFile::deserialize(output.locations.at(0));
  std::fstream file(
      outputFile.path, std::ios::in | std::ios::out | std::ios::binary);
  file.seekp(sizeof(file_exchange::PageSize));
  file.put('z');
  file.close();

  folly::ManualExecutor executor;
  auto client = std::make_shared<InMemoryExchangeClient>(
      "consumer", 0, 4 * kPageBytes, 1, 0, pool_.get(), &executor);
  SCOPE_EXIT {
    client->close();
    runUntilIdle(executor);
  };
  client->addRemoteTaskId(output.locations.at(0));
  client->noMoreRemoteTasks();
  runUntilIdle(executor);
  EXPECT_EQ(pool_->usedBytes(), 0);
  bool atEnd = false;
  ContinueFuture future;
  VELOX_ASSERT_THROW(
      client->next(0, 1, &atEnd, &future), "File exchange checksum mismatch");
}

TEST_F(FileExchangeTest, clientPreservesMemoryLimitErrorCode) {
  constexpr int64_t kPageBytes = 2 << 20;
  for (const bool queuePageBeforeFailure : {false, true}) {
    SCOPED_TRACE(queuePageBeforeFailure);
    FileExchangeSink sink(
        rootDirectory_,
        fmt::format("exchange_{}", queuePageBeforeFailure),
        "task",
        1);
    if (queuePageBeforeFailure) {
      sink.append(0, std::string(4'096, 'a'));
    }
    sink.append(0, std::string(kPageBytes, 'b'));
    const auto output = sink.finish();
    auto limitedRoot =
        memory::memoryManager()->addRootPool("limited-client", 1 << 20);
    auto limitedPool = limitedRoot->addLeafChild("exchange");
    folly::ManualExecutor executor;
    auto client = std::make_shared<InMemoryExchangeClient>(
        "consumer", 0, 4 * kPageBytes, 1, 0, limitedPool.get(), &executor);
    SCOPE_EXIT {
      client->close();
      runUntilIdle(executor);
    };
    client->addRemoteTaskId(output.locations.at(0));
    client->noMoreRemoteTasks();
    bool atEnd = false;
    ContinueFuture future;
    EXPECT_THAT(client->next(0, 1, &atEnd, &future), testing::IsEmpty());
    ASSERT_TRUE(future.valid());
    EXPECT_FALSE(future.isReady());
    runUntilIdle(executor);
    EXPECT_TRUE(future.isReady());
    EXPECT_EQ(limitedPool->usedBytes(), 0);
    // A later error must not replace the original allocation failure.
    client->queue()->setError("secondary failure");
    for (int i = 0; i < 2; ++i) {
      VELOX_ASSERT_THROW_CODE(
          client->next(0, 1, &atEnd, &future),
          error_code::kMemCapExceeded,
          "Exceeded memory pool capacity");
      EXPECT_TRUE(atEnd);
    }
  }
}

TEST_F(FileExchangeTest, queueStringErrorsWakeConsumersAndKeepFirstError) {
  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  bool atEnd = false;
  ContinueFuture future;
  auto stalePromise = ContinuePromise::makeEmpty();
  {
    std::lock_guard<std::mutex> lock(queue->mutex());
    EXPECT_THAT(
        queue->dequeueLocked(0, 1, &atEnd, &future, &stalePromise),
        testing::IsEmpty());
  }
  EXPECT_FALSE(future.isReady());
  queue->setError("first failure");
  queue->setError("later failure");
  queue->setError(
      std::make_exception_ptr(std::runtime_error("typed secondary failure")));
  EXPECT_TRUE(future.isReady());
  std::lock_guard<std::mutex> lock(queue->mutex());
  VELOX_ASSERT_THROW_CODE(
      queue->dequeueLocked(0, 1, &atEnd, &future, &stalePromise),
      error_code::kInvalidState,
      "first failure");
  EXPECT_TRUE(atEnd);
}

TEST_F(FileExchangeTest, queuePreservesExceptionTypeAndIdentity) {
  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  const auto error =
      std::make_exception_ptr(std::runtime_error("source failure"));
  queue->setError(error);
  queue->setError("secondary failure");
  bool atEnd = false;
  ContinueFuture future;
  auto stalePromise = ContinuePromise::makeEmpty();
  std::lock_guard<std::mutex> lock(queue->mutex());
  try {
    queue->dequeueLocked(0, 1, &atEnd, &future, &stalePromise);
    FAIL() << "Expected source exception";
  } catch (const std::runtime_error& received) {
    EXPECT_STREQ(received.what(), "source failure");
    EXPECT_EQ(std::current_exception(), error);
  }
  EXPECT_TRUE(atEnd);
}

#ifndef NDEBUG
TEST_F(FileExchangeTest, allocationCanCancelWithoutHoldingQueueLock) {
  constexpr int64_t kPageBytes = 4'096;
  FileExchangeSink sink(
      fmt::format("faulty:{}", rootDirectory_), "exchange", "task", 1);
  sink.append(0, std::string(kPageBytes, 'x'));
  const auto output = sink.finish();
  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  FileExchangeSource source(output.locations.at(0), 0, queue, pool_.get());
  int numPayloadReads = 0;
  tests::utils::faultyFileSystem()->setFileInjectionHook(
      [&](tests::utils::FaultFileOperation* operation) {
        if (operation->type == tests::utils::FaultFileOperation::Type::kRead &&
            static_cast<tests::utils::FaultFileReadOperation*>(operation)
                    ->length == kPageBytes) {
          ++numPayloadReads;
        }
      });
  const bool wasEnabled = common::testutil::TestValue::enabled();
  common::testutil::TestValue::enable();
  SCOPE_EXIT {
    if (!wasEnabled) {
      common::testutil::TestValue::disable();
    }
  };
  int numReservations = 0;
  SCOPED_TESTVALUE_SET(
      "facebook::velox::memory::MemoryPoolImpl::reserveThreadSafe",
      std::function<void(memory::MemoryPool*)>([&](memory::MemoryPool* pool) {
        if (pool == pool_.get()) {
          ++numReservations;
          // Memory arbitration may cancel the task while allocating a page.
          source.close();
          queue->close();
        }
      }));
  EXPECT_EQ(source.request(kPageBytes, {}).get().bytes, 0);
  EXPECT_EQ(numReservations, 1);
  EXPECT_EQ(numPayloadReads, 0);
  EXPECT_EQ(pool_->usedBytes(), 0);
}
#endif

TEST_F(FileExchangeTest, closeDuringPayloadReadDiscardsPage) {
  constexpr int64_t kPageBytes = 4'096;
  FileExchangeSink sink(
      fmt::format("faulty:{}", rootDirectory_), "exchange", "task", 1);
  sink.append(0, std::string(kPageBytes, 'x'));
  sink.append(0, std::string(kPageBytes, 'y'));
  const auto output = sink.finish();
  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  FileExchangeSource source(output.locations.at(0), 0, queue, pool_.get());
  int numPayloadReads = 0;
  tests::utils::faultyFileSystem()->setFileInjectionHook(
      [&](tests::utils::FaultFileOperation* operation) {
        if (operation->type != tests::utils::FaultFileOperation::Type::kRead) {
          return;
        }
        auto* read =
            static_cast<tests::utils::FaultFileReadOperation*>(operation);
        if (read->length == kPageBytes) {
          ++numPayloadReads;
          // Cancellation may run while an I/O request owns its payload buffer.
          source.close();
          queue->close();
        }
      });
  const auto response = source.request(2 * kPageBytes, {}).get();
  EXPECT_EQ(numPayloadReads, 1);
  EXPECT_EQ(response.bytes, 0);
  EXPECT_FALSE(response.atEnd);
  EXPECT_EQ(pool_->usedBytes(), 0);
  std::lock_guard<std::mutex> lock(queue->mutex());
  EXPECT_EQ(queue->totalBytes(), 0);
  EXPECT_EQ(queue->receivedPages(), 0);
}

TEST_F(FileExchangeTest, failedRequestReleasesClaim) {
  const file_exchange::ExchangeOutputFile outputFile{
      .path = fmt::format("{}/missing.bin", rootDirectory_),
      .size = 0,
      .checksum = 0,
  };
  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  FileExchangeSource source(outputFile.serialize(), 0, queue, pool_.get());
  {
    std::lock_guard<std::mutex> lock(queue->mutex());
    ASSERT_TRUE(source.shouldRequestLocked());
    EXPECT_FALSE(source.shouldRequestLocked());
  }
  VELOX_ASSERT_THROW(source.requestDataSizes({}).get(), "No such file");
  std::lock_guard<std::mutex> lock(queue->mutex());
  EXPECT_TRUE(source.shouldRequestLocked());
}

TEST_F(FileExchangeTest, payloadAllocationFailureReleasesMemory) {
  constexpr int64_t kPageBytes = 2 << 20;
  FileExchangeSink sink(rootDirectory_, "exchange", "task", 1);
  sink.append(0, std::string(kPageBytes, 'x'));
  const auto output = sink.finish();
  auto limitedRoot = memory::memoryManager()->addRootPool("limited", 1 << 20);
  auto limitedPool = limitedRoot->addLeafChild("reader");
  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  FileExchangeSource source(
      output.locations.at(0), 0, queue, limitedPool.get());
  VELOX_ASSERT_THROW(
      source.request(kPageBytes, {}).get(), "Exceeded memory pool capacity");
  EXPECT_EQ(limitedPool->usedBytes(), 0);
}

TEST_F(FileExchangeTest, payloadReadFailureReleasesMemory) {
  constexpr int64_t kPageBytes = 4'096;
  FileExchangeSink sink(
      fmt::format("faulty:{}", rootDirectory_), "exchange", "task", 1);
  sink.append(0, std::string(kPageBytes, 'x'));
  const auto output = sink.finish();
  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  FileExchangeSource source(output.locations.at(0), 0, queue, pool_.get());
  tests::utils::faultyFileSystem()->setFileInjectionHook(
      [&](tests::utils::FaultFileOperation* operation) {
        if (operation->type != tests::utils::FaultFileOperation::Type::kRead) {
          return;
        }
        auto* read =
            static_cast<tests::utils::FaultFileReadOperation*>(operation);
        if (read->length == kPageBytes) {
          EXPECT_EQ(pool_->usedBytes(), kPageBytes);
          VELOX_FAIL("Injected payload read failure");
        }
      });
  VELOX_ASSERT_THROW(
      source.request(kPageBytes, {}).get(), "Injected payload read failure");
  EXPECT_EQ(pool_->usedBytes(), 0);
}

TEST_F(FileExchangeTest, payloadOwnershipTracksPoolAndOutlivesSource) {
  constexpr int64_t kPageBytes = 4'096;
  FileExchangeSink sink(rootDirectory_, "exchange", "task", 1);
  sink.append(0, std::string(kPageBytes, 'x'));
  const auto output = sink.finish();

  auto leafPool = rootPool_->addLeafChild("payload");
  std::weak_ptr<memory::MemoryPool> weakPool = leafPool;
  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  std::unique_ptr<folly::IOBuf> clone;
  {
    FileExchangeSource source(output.locations.at(0), 0, queue, leafPool.get());
    EXPECT_EQ(source.request(kPageBytes, {}).get().bytes, kPageBytes);
    EXPECT_EQ(leafPool->usedBytes(), kPageBytes);
    bool atEnd = false;
    ContinueFuture future;
    auto stalePromise = ContinuePromise::makeEmpty();
    std::vector<std::unique_ptr<SerializedPageBase>> pages;
    {
      std::lock_guard<std::mutex> lock(queue->mutex());
      pages =
          queue->dequeueLocked(0, kPageBytes, &atEnd, &future, &stalePromise);
    }
    ASSERT_EQ(pages.size(), 1);
    clone = pages.front()->getIOBuf();
    pages.clear();
    EXPECT_EQ(leafPool->usedBytes(), kPageBytes);
    source.close();
    queue->close();
  }
  leafPool.reset();
  ASSERT_FALSE(weakPool.expired());
  EXPECT_EQ(weakPool.lock()->usedBytes(), kPageBytes);
  EXPECT_EQ(clone->length(), kPageBytes);
  clone.reset();
  EXPECT_TRUE(weakPool.expired());
}

TEST_F(FileExchangeTest, exchangeOutputFileRoundTrip) {
  const file_exchange::ExchangeOutputFile expected{
      .path = R"(test://root/path?size=inside&note="quoted")",
      .size = 123,
      .checksum = 456,
  };

  const auto serializedOutput = expected.serialize();
  EXPECT_NE(serializedOutput.find(R"("checksum":456)"), std::string::npos);
  EXPECT_EQ(serializedOutput.find(R"("crc32c")"), std::string::npos);
  EXPECT_TRUE(file_exchange::ExchangeOutputFile::serialized(serializedOutput));
  EXPECT_FALSE(file_exchange::ExchangeOutputFile::serialized("file://path"));
  const auto actual =
      file_exchange::ExchangeOutputFile::deserialize(serializedOutput);
  EXPECT_EQ(actual.path, expected.path);
  EXPECT_EQ(actual.size, expected.size);
  EXPECT_EQ(actual.checksum, expected.checksum);
}

TEST_F(FileExchangeTest, pageSizeRoundTrip) {
  for (const uint64_t size : std::initializer_list<uint64_t>{
           0,
           1,
           1'024,
           std::numeric_limits<uint64_t>::max(),
       }) {
    EXPECT_EQ(
        file_exchange::decodePageSize(file_exchange::encodePageSize(size)),
        size);
  }
}

TEST_F(FileExchangeTest, usesRegisteredFileSystemForStoragePath) {
  const auto rootDirectory = fmt::format("faulty:{}", rootDirectory_);
  FileExchangeSink sink(rootDirectory, "exchange", "task", 1);
  sink.append(0, "page");
  const auto output = sink.finish();

  EXPECT_EQ(
      file_exchange::ExchangeOutputFile::deserialize(output.locations.at(0))
          .path,
      fmt::format("faulty:{}/exchange/partition_0/task.bin", rootDirectory_));
  EXPECT_TRUE(
      std::filesystem::exists(
          fmt::format("{}/exchange/partition_0/task.bin", rootDirectory_)));

  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  {
    std::lock_guard<std::mutex> lock(queue->mutex());
    queue->addSourceLocked();
  }
  queue->noMoreSources();

  FileExchangeSource source(output.locations.at(0), 0, queue, pool_.get());
  const auto response =
      source.request(std::numeric_limits<uint32_t>::max(), {}).get();
  EXPECT_TRUE(response.atEnd);

  bool atEnd = false;
  ContinueFuture future;
  auto stalePromise = ContinuePromise::makeEmpty();
  std::vector<std::unique_ptr<SerializedPageBase>> pages;
  {
    std::lock_guard<std::mutex> lock(queue->mutex());
    pages = queue->dequeueLocked(
        0,
        std::numeric_limits<uint32_t>::max(),
        &atEnd,
        &future,
        &stalePromise);
  }
  ASSERT_TRUE(atEnd);
  ASSERT_EQ(pages.size(), 1);
  EXPECT_EQ(pageData(*pages[0]), "page");
}

TEST_F(FileExchangeTest, chainedPageIsOnePage) {
  FileExchangeSink sink(rootDirectory_, "exchange", "task", 1);
  auto page = folly::IOBuf::copyBuffer("first ", 6);
  page->appendToChain(folly::IOBuf::copyBuffer("second", 6));
  sink.append(0, std::move(page));
  const auto output = sink.finish();

  const auto stats = sink.stats();
  EXPECT_EQ(stats.at("totalBytesWritten"), 12);
  EXPECT_EQ(stats.at("totalPages"), 1);

  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  {
    std::lock_guard<std::mutex> lock(queue->mutex());
    queue->addSourceLocked();
  }
  queue->noMoreSources();

  FileExchangeSource source(output.locations.at(0), 0, queue, pool_.get());
  const auto response =
      source.request(std::numeric_limits<uint32_t>::max(), {}).get();
  EXPECT_TRUE(response.atEnd);

  bool atEnd = false;
  ContinueFuture future;
  auto stalePromise = ContinuePromise::makeEmpty();
  std::vector<std::unique_ptr<SerializedPageBase>> pages;
  {
    std::lock_guard<std::mutex> lock(queue->mutex());
    pages = queue->dequeueLocked(
        0,
        std::numeric_limits<uint32_t>::max(),
        &atEnd,
        &future,
        &stalePromise);
  }

  ASSERT_TRUE(atEnd);
  ASSERT_EQ(pages.size(), 1);
  EXPECT_EQ(pageData(*pages[0]), "first second");
}

TEST_F(FileExchangeTest, closePreventsReads) {
  FileExchangeSink sink(rootDirectory_, "exchange", "task", 1);
  sink.append(0, "page");
  const auto output = sink.finish();

  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  FileExchangeSource source(output.locations.at(0), 0, queue, pool_.get());

  source.close();

  const auto response =
      source.request(std::numeric_limits<uint32_t>::max(), {}).get();
  EXPECT_EQ(response.bytes, 0);
  EXPECT_FALSE(response.atEnd);
}

TEST_F(FileExchangeTest, emptyPartitionsHaveNoCommittedLocation) {
  FileExchangeSink sink(rootDirectory_, "exchange", "task", 2);
  sink.append(1, "page");
  const auto output = sink.finish();

  EXPECT_EQ(output.locations.size(), 1);
  EXPECT_EQ(output.locations.count(0), 0);
  EXPECT_EQ(output.locations.count(1), 1);
}

TEST_F(FileExchangeTest, missingFileIsRejected) {
  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  const file_exchange::ExchangeOutputFile outputFile{
      .path = fmt::format("{}/missing.bin", rootDirectory_),
      .size = 0,
      .checksum = 0,
  };
  FileExchangeSource source(outputFile.serialize(), 0, queue, pool_.get());

  VELOX_ASSERT_THROW(
      source.request(std::numeric_limits<uint32_t>::max(), {}).get(),
      "No such file");
}

TEST_F(FileExchangeTest, corruptedFileIsRejected) {
  FileExchangeSink sink(rootDirectory_, "exchange", "task", 1);
  sink.append(0, "page");
  const auto output = sink.finish();

  const auto filePath =
      fmt::format("{}/exchange/partition_0/task.bin", rootDirectory_);
  std::fstream file(filePath, std::ios::in | std::ios::out | std::ios::binary);
  file.seekp(sizeof(file_exchange::PageSize));
  file.put('P');
  file.close();

  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  {
    std::lock_guard<std::mutex> lock(queue->mutex());
    queue->addSourceLocked();
  }
  queue->noMoreSources();

  FileExchangeSource source(output.locations.at(0), 0, queue, pool_.get());
  VELOX_ASSERT_THROW(
      source.request(std::numeric_limits<uint32_t>::max(), {}).get(),
      "File exchange checksum mismatch");
}

TEST_F(FileExchangeTest, incorrectFileSizeIsRejected) {
  FileExchangeSink sink(rootDirectory_, "exchange", "task", 1);
  sink.append(0, "page");
  sink.finish();

  const auto filePath =
      fmt::format("{}/exchange/partition_0/task.bin", rootDirectory_);
  const auto contents = fileContents({"page"});
  const file_exchange::ExchangeOutputFile outputFile{
      .path = filePath,
      .size = contents.size() + 1,
      .checksum = checksum(contents),
  };
  const auto location = outputFile.serialize();

  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  {
    std::lock_guard<std::mutex> lock(queue->mutex());
    queue->addSourceLocked();
  }
  queue->noMoreSources();

  FileExchangeSource source(location, 0, queue, pool_.get());
  VELOX_ASSERT_THROW(
      source.request(std::numeric_limits<uint32_t>::max(), {}).get(),
      "File exchange size mismatch");
}

TEST_F(FileExchangeTest, truncatedPageIsRejected) {
  const auto filePath = fmt::format("{}/truncated.bin", rootDirectory_);
  std::ofstream file(filePath, std::ios::binary);
  const auto encodedPageSize = file_exchange::encodePageSize(10);
  file.write(
      reinterpret_cast<const char*>(&encodedPageSize),
      sizeof(file_exchange::PageSize));
  file.write("short", 5);
  file.close();

  auto queue = std::make_shared<ExchangeQueue>(1, 0);
  {
    std::lock_guard<std::mutex> lock(queue->mutex());
    queue->addSourceLocked();
  }
  queue->noMoreSources();

  FileExchangeSource source(
      fileLocation(filePath, fileContents({"short"})), 0, queue, pool_.get());
  VELOX_ASSERT_THROW(
      source.request(std::numeric_limits<uint32_t>::max(), {}).get(),
      "Incomplete page payload");
}

} // namespace
} // namespace facebook::velox::exec
