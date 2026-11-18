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

#include "velox/exec/FileExchangeSource.h"

#include <mutex>

#include <folly/ScopeGuard.h>
#include <folly/hash/Checksum.h>
#include <folly/io/IOBuf.h>
#include "velox/common/base/Exceptions.h"
#include "velox/common/future/VeloxPromise.h"
#include "velox/exec/FileExchangeFormat.h"
#include "velox/exec/SerializedPage.h"

namespace facebook::velox::exec {
namespace {

// Keeps the pool alive until all IOBuf clones have released the payload.
struct PageBufferOwner {
  // Pool responsible for freeing the allocation.
  std::shared_ptr<memory::MemoryPool> pool;
  // Allocation size before pool alignment.
  uint64_t size;
};

std::unique_ptr<folly::IOBuf> allocatePageBuffer(
    const std::shared_ptr<memory::MemoryPool>& pool,
    uint64_t size) {
  auto owner = std::make_unique<PageBufferOwner>(pool, size);
  auto* data = pool->allocate(size);
  // takeOwnership invokes the deleter on construction failure as well.
  return folly::IOBuf::takeOwnership(
      data,
      size,
      0,
      [](void* buffer, void* userData) {
        const std::unique_ptr<PageBufferOwner> owner{
            static_cast<PageBufferOwner*>(userData)};
        owner->pool->free(buffer, owner->size);
      },
      owner.release());
}

} // namespace

FileExchangeSource::FileExchangeSource(
    const std::string& taskId,
    int32_t destination,
    std::shared_ptr<ExchangeQueue> queue,
    memory::MemoryPool* pool)
    : ExchangeSource(taskId, destination, queue, pool) {
  auto outputFile = file_exchange::ExchangeOutputFile::deserialize(taskId);
  filePath_ = std::move(outputFile.path);
  expectedFileSize_ = outputFile.size;
  expectedChecksum_ = outputFile.checksum;
}

bool FileExchangeSource::shouldRequestLocked() {
  if (atEnd_ || closed_) {
    return false;
  }
  return !requestPending_.exchange(true);
}

void FileExchangeSource::openFile() {
  if (file_ != nullptr) {
    return;
  }
  fileSystem_ = filesystems::getFileSystem(filePath_, nullptr);
  file_ = fileSystem_->openFileForRead(filePath_);
  const auto fileSize = file_->size();
  VELOX_CHECK_EQ(
      fileSize,
      expectedFileSize_,
      "File exchange size mismatch for {}",
      filePath_);
}

std::optional<uint64_t> FileExchangeSource::readPageSize() {
  if (nextPageSize_.has_value()) {
    return nextPageSize_;
  }
  if (fileBytesRead_ == expectedFileSize_) {
    return std::nullopt;
  }
  VELOX_CHECK_GE(
      expectedFileSize_ - fileBytesRead_,
      sizeof(file_exchange::PageSize),
      "Incomplete page-size header in file: {}",
      filePath_);

  file_exchange::PageSize encodedPageSize;
  const auto encodedPageSizeData = file_->pread(
      fileBytesRead_, sizeof(file_exchange::PageSize), &encodedPageSize);
  VELOX_CHECK_EQ(
      encodedPageSizeData.size(),
      sizeof(file_exchange::PageSize),
      "Incomplete page-size header in file: {}",
      filePath_);
  fileChecksum_ = folly::crc32c(
      reinterpret_cast<const uint8_t*>(&encodedPageSize),
      sizeof(file_exchange::PageSize),
      fileChecksum_);
  fileBytesRead_ += sizeof(file_exchange::PageSize);

  const auto pageSize = file_exchange::decodePageSize(encodedPageSize);
  VELOX_CHECK_GT(pageSize, 0, "Empty page payload in file: {}", filePath_);
  VELOX_CHECK_LE(
      pageSize,
      expectedFileSize_ - fileBytesRead_,
      "Incomplete page payload in file: {}",
      filePath_);
  nextPageSize_ = pageSize;
  return nextPageSize_;
}

std::unique_ptr<SerializedPageBase> FileExchangeSource::readPage() {
  const auto pageSize = nextPageSize_.value();
  // Pool allocation can arbitrate memory and must run outside the queue lock.
  auto buffer = allocatePageBuffer(pool_, pageSize);
  if (closed_) {
    return nullptr;
  }
  const auto pageData =
      file_->pread(fileBytesRead_, pageSize, buffer->writableData());
  VELOX_CHECK_EQ(
      pageData.size(),
      pageSize,
      "Incomplete page payload in file: {}",
      filePath_);
  fileChecksum_ =
      folly::crc32c(buffer->writableData(), pageSize, fileChecksum_);
  fileBytesRead_ += pageSize;
  nextPageSize_.reset();
  buffer->append(pageSize);
  return std::make_unique<PrestoSerializedPage>(std::move(buffer));
}

void FileExchangeSource::verifyChecksum() const {
  VELOX_CHECK_EQ(
      fileChecksum_,
      expectedChecksum_,
      "File exchange checksum mismatch for {}",
      filePath_);
}

bool FileExchangeSource::enqueuePage(std::unique_ptr<SerializedPageBase> page) {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> lock(queue_->mutex());
    if (closed_) {
      return false;
    }
    queue_->enqueueLocked(std::move(page), promises);
  }
  for (auto& promise : promises) {
    promise.setValue();
  }
  return true;
}

bool FileExchangeSource::enqueueEnd() {
  std::vector<ContinuePromise> promises;
  {
    std::lock_guard<std::mutex> lock(queue_->mutex());
    if (closed_) {
      return false;
    }
    if (endEnqueued_) {
      return true;
    }
    verifyChecksum();
    endEnqueued_ = true;
    atEnd_ = true;
    queue_->enqueueLocked(nullptr, promises);
  }
  for (auto& promise : promises) {
    promise.setValue();
  }
  return true;
}

folly::SemiFuture<ExchangeSource::Response> FileExchangeSource::request(
    uint32_t maxBytes,
    std::chrono::microseconds /*maxWait*/) {
  SCOPE_EXIT {
    requestPending_ = false;
  };

  // Surface I/O and allocation errors through the future so the client can
  // fail the queue and release any pages already buffered there.
  return folly::makeSemiFutureWith([&]() -> Response {
    int64_t bytes{0};
    bool atEnd{false};
    std::vector<int64_t> remainingBytes;
    if (!closed_) {
      openFile();
    }
    while (!closed_) {
      const auto pageSize = readPageSize();
      if (!pageSize.has_value()) {
        atEnd = enqueueEnd();
        break;
      }
      if (maxBytes == 0 ||
          (bytes > 0 &&
           (bytes >= maxBytes || pageSize.value() > maxBytes - bytes))) {
        remainingBytes.push_back(pageSize.value());
        break;
      }
      auto page = readPage();
      if (page == nullptr) {
        break;
      }
      const auto pageBytes = page->size();
      if (!enqueuePage(std::move(page))) {
        break;
      }
      bytes += pageBytes;
    }
    return Response{
        .bytes = bytes,
        .atEnd = atEnd,
        .remainingBytes = std::move(remainingBytes)};
  });
}

void FileExchangeSource::close() {
  std::lock_guard<std::mutex> lock(queue_->mutex());
  closed_ = true;
}

// --- Factory ---

std::shared_ptr<ExchangeSource> FileExchangeSourceFactory::operator()(
    const std::string& taskId,
    int32_t destination,
    std::shared_ptr<ExchangeQueue> queue,
    memory::MemoryPool* pool) {
  if (file_exchange::ExchangeOutputFile::serialized(taskId)) {
    return std::make_shared<FileExchangeSource>(
        taskId, destination, std::move(queue), pool);
  }
  return nullptr;
}

} // namespace facebook::velox::exec
