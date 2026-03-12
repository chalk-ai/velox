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

#include <folly/dynamic.h>

#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/common/compression/Compression.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::json {

using common::CompressionKind;
using dwio::common::BufferedInput;
using dwio::common::ReaderOptions;
using dwio::common::RowReaderOptions;
using dwio::common::TypeWithId;
using memory::MemoryPool;

struct JsonFileContents {
  JsonFileContents(
      MemoryPool& pool,
      RowTypePtr schema,
      std::unique_ptr<BufferedInput> input,
      uint64_t fileLength,
      CompressionKind compression,
      dwio::common::compression::CompressionOptions compressionOptions)
      : pool(pool),
        schema(std::move(schema)),
        input(std::move(input)),
        fileLength(fileLength),
        compression(compression),
        compressionOptions(std::move(compressionOptions)) {}

  MemoryPool& pool;
  RowTypePtr schema;
  std::unique_ptr<BufferedInput> input;
  uint64_t fileLength;
  CompressionKind compression;
  dwio::common::compression::CompressionOptions compressionOptions;
};

class JsonReader : public dwio::common::Reader {
 public:
  JsonReader(
      const ReaderOptions& options,
      std::unique_ptr<BufferedInput> input);

  std::optional<uint64_t> numberOfRows() const override {
    return std::nullopt;
  }

  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t index) const override;

  const RowTypePtr& rowType() const override;

  const std::shared_ptr<const TypeWithId>& typeWithId() const override;

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const RowReaderOptions& options = {}) const override;

 private:
  ReaderOptions options_;
  std::shared_ptr<JsonFileContents> contents_;
  mutable std::shared_ptr<const TypeWithId> typeWithId_;
};

class JsonRowReader : public dwio::common::RowReader {
 public:
  JsonRowReader(
      std::shared_ptr<JsonFileContents> contents,
      const RowReaderOptions& options);

  uint64_t next(
      uint64_t size,
      VectorPtr& result,
      const dwio::common::Mutation* mutation = nullptr) override;

  int64_t nextRowNumber() override;

  int64_t nextReadSize(uint64_t size) override;

  void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override;

  void resetFilterCaches() override;

  std::optional<size_t> estimatedRowSize() const override;

 private:
  // Reads the next newline-delimited line into `line`.
  // Returns false at EOF. Silently skips empty lines.
  bool readLine(std::string& line);

  // Reads bytes from the stream, refilling the buffer as needed.
  // Returns false when the underlying stream is exhausted.
  bool nextByte(char& c);

  void populateRow(
      const folly::dynamic& obj,
      RowVector* rowVec,
      vector_size_t row);

  void setFieldValue(
      const folly::dynamic& val,
      const TypePtr& type,
      BaseVector* vec,
      vector_size_t row);

  void setStringValue(
      FlatVector<StringView>* flat,
      vector_size_t row,
      const std::string& str);

  std::shared_ptr<JsonFileContents> contents_;
  std::unique_ptr<dwio::common::SeekableInputStream> stream_;
  std::shared_ptr<velox::common::ScanSpec> scanSpec_;
  uint64_t currentRow_{0};
  bool atEOF_{false};
  uint64_t limit_;

  // Buffer state for reading chunks from stream via Next().
  const void* bufData_{nullptr};
  int32_t bufSize_{0};
  int32_t bufPos_{0};
};

} // namespace facebook::velox::json
