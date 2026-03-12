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

#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Writer.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"

namespace facebook::velox::json {

struct WriterOptions : public dwio::common::WriterOptions {
  // Flush the output buffer after accumulating this many bytes.
  uint64_t flushThresholdBytes = 8 << 20; // 8 MB
};

/// Serializes Velox RowVectors as newline-delimited JSON (NDJSON) and writes
/// them to a FileSink. One JSON object per row, one row per line.
class JsonWriter : public dwio::common::Writer {
 public:
  JsonWriter(
      RowTypePtr schema,
      std::unique_ptr<dwio::common::FileSink> sink,
      const std::shared_ptr<WriterOptions>& options);

  ~JsonWriter() override = default;

  void write(const VectorPtr& data) override;

  void flush() override;

  bool finish() override {
    close();
    return true;
  }

  void close() override;

  void abort() override;

 private:
  folly::dynamic vectorToJson(
      const DecodedVector& decoded,
      const TypePtr& type,
      vector_size_t row);

  void flushBuffer();

  const RowTypePtr schema_;
  const std::unique_ptr<dwio::common::FileSink> sink_;
  memory::MemoryPool* pool_;
  const uint64_t flushThresholdBytes_;
  std::string buffer_;
};

class JsonWriterFactory : public dwio::common::WriterFactory {
 public:
  JsonWriterFactory() : WriterFactory(dwio::common::FileFormat::JSON) {}

  std::unique_ptr<dwio::common::Writer> createWriter(
      std::unique_ptr<dwio::common::FileSink> sink,
      const std::shared_ptr<dwio::common::WriterOptions>& options) override;

  std::unique_ptr<dwio::common::WriterOptions> createWriterOptions() override;
};

} // namespace facebook::velox::json
