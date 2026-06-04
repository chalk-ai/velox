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

#include "velox/dwio/json/reader/JsonReader.h"

#include <folly/json.h>

#include "velox/common/encode/Base64.h"
#include "velox/dwio/common/exception/Exceptions.h"
#include "velox/type/TimestampConversion.h"

namespace facebook::velox::json {

namespace {

constexpr const char* kJsonCompressionExtensionGzip = ".gz";
constexpr const char* kJsonCompressionExtensionZst = ".zst";

bool endsWith(const std::string& str, const std::string& suffix) {
  return str.size() >= suffix.size() &&
      str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

void detectCompression(
    const std::string& filename,
    CompressionKind& kind,
    dwio::common::compression::CompressionOptions& compressionOptions) {
  if (endsWith(filename, kJsonCompressionExtensionGzip)) {
    kind = CompressionKind::CompressionKind_GZIP;
    compressionOptions.format.zlib.windowBits = 15;
  } else if (endsWith(filename, kJsonCompressionExtensionZst)) {
    kind = CompressionKind::CompressionKind_ZSTD;
  } else {
    kind = CompressionKind::CompressionKind_NONE;
  }
}

} // namespace

// ---- JsonReader ----

JsonReader::JsonReader(
    const ReaderOptions& options,
    std::unique_ptr<BufferedInput> input)
    : options_(options) {
  auto schema = options_.fileSchema();
  VELOX_USER_CHECK_NOT_NULL(schema, "File schema for JSON must be set.");
  VELOX_USER_CHECK(schema->isRow(), "File schema for JSON must be a ROW type.");

  auto fileLength = std::min(
      options_.tailLocation(),
      static_cast<uint64_t>(input->getInputStream()->getLength()));

  CompressionKind compression;
  dwio::common::compression::CompressionOptions compressionOptions;
  detectCompression(input->getInputStream()->getName(), compression, compressionOptions);

  contents_ = std::make_shared<JsonFileContents>(
      options_.memoryPool(),
      schema,
      std::move(input),
      fileLength,
      compression,
      std::move(compressionOptions));
}

std::unique_ptr<dwio::common::ColumnStatistics> JsonReader::columnStatistics(
    uint32_t /*index*/) const {
  return nullptr;
}

const RowTypePtr& JsonReader::rowType() const {
  return contents_->schema;
}

const std::shared_ptr<const TypeWithId>& JsonReader::typeWithId() const {
  if (!typeWithId_) {
    typeWithId_ = TypeWithId::create(rowType());
  }
  return typeWithId_;
}

std::unique_ptr<dwio::common::RowReader> JsonReader::createRowReader(
    const RowReaderOptions& options) const {
  return std::make_unique<JsonRowReader>(contents_, options);
}

// ---- JsonRowReader ----

JsonRowReader::JsonRowReader(
    std::shared_ptr<JsonFileContents> contents,
    const RowReaderOptions& options)
    : contents_(std::move(contents)),
      scanSpec_(options.scanSpec()),
      limit_(options.limit()) {
  const auto pos = options.offset();

  if (contents_->compression == CompressionKind::CompressionKind_NONE) {
    stream_ = contents_->input->read(
        pos,
        contents_->fileLength - pos,
        dwio::common::LogType::STREAM);
    // If starting mid-file, skip the partial first line (split boundary).
    if (pos != 0) {
      std::string discard;
      readLine(discard);
    }
  } else {
    // Compressed files: the first split reads the whole file; others are empty.
    if (pos != 0) {
      atEOF_ = true;
      return;
    }

    const auto blockSize = std::numeric_limits<unsigned int>::max();
    auto rawStream = contents_->input->loadCompleteFile();
    const auto name = rawStream->getName();
    stream_ = createDecompressor(
        contents_->compression,
        std::move(rawStream),
        blockSize,
        contents_->pool,
        contents_->compressionOptions,
        fmt::format("JSON Reader: Stream {}", name),
        nullptr,
        true,
        contents_->fileLength);
  }
}

bool JsonRowReader::nextByte(char& c) {
  while (bufPos_ >= bufSize_) {
    if (!stream_->Next(&bufData_, &bufSize_)) {
      return false;
    }
    bufPos_ = 0;
  }
  c = static_cast<const char*>(bufData_)[bufPos_++];
  return true;
}

bool JsonRowReader::readLine(std::string& line) {
  line.clear();
  char c;
  bool gotAny = false;
  while (nextByte(c)) {
    if (c == '\n') {
      // Return even if line is empty — caller decides what to do.
      return true;
    }
    if (c != '\r') {
      line += c;
      gotAny = true;
    }
  }
  // EOF: return true if we read any non-newline content.
  return gotAny;
}

uint64_t JsonRowReader::next(
    uint64_t size,
    VectorPtr& result,
    const dwio::common::Mutation* mutation) {
  if (atEOF_) {
    return 0;
  }

  auto rowVec = BaseVector::create<RowVector>(
      contents_->schema, static_cast<vector_size_t>(size), &contents_->pool);

  vector_size_t rowsRead = 0;
  std::string line;

  while (!atEOF_ && rowsRead < static_cast<vector_size_t>(size)) {
    bool hasData = readLine(line);
    if (!hasData) {
      atEOF_ = true;
      break;
    }
    if (line.empty()) {
      // Silently skip blank lines.
      continue;
    }

    folly::dynamic obj = folly::parseJson(line);
    VELOX_USER_CHECK(
        obj.isObject(),
        "JSON reader expects each line to be a JSON object, got: {}",
        line);

    populateRow(obj, rowVec.get(), rowsRead);
    ++rowsRead;
    ++currentRow_;
  }

  rowVec->resize(rowsRead);
  result = projectColumns(rowVec, *scanSpec_, mutation);
  return rowsRead;
}

void JsonRowReader::populateRow(
    const folly::dynamic& obj,
    RowVector* rowVec,
    vector_size_t row) {
  const auto& schema = contents_->schema;
  for (size_t i = 0; i < schema->size(); ++i) {
    const auto& fieldName = schema->nameOf(i);
    auto* childVec = rowVec->childAt(i).get();

    if (obj.count(fieldName) == 0) {
      childVec->setNull(row, true);
    } else {
      setFieldValue(obj[fieldName], schema->childAt(i), childVec, row);
    }
  }
}

void JsonRowReader::setStringValue(
    FlatVector<StringView>* flat,
    vector_size_t row,
    const std::string& str) {
  StringView sv(str.data(), str.size());
  if (sv.isInline()) {
    flat->set(row, sv);
  } else {
    auto buf = AlignedBuffer::allocate<char>(str.size(), flat->pool());
    memcpy(buf->asMutable<char>(), str.data(), str.size());
    flat->addStringBuffer(buf);
    flat->set(row, StringView(buf->as<char>(), str.size()));
  }
}

void JsonRowReader::setFieldValue(
    const folly::dynamic& val,
    const TypePtr& type,
    BaseVector* vec,
    vector_size_t row) {
  if (val.isNull()) {
    vec->setNull(row, true);
    return;
  }

  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      vec->asFlatVector<bool>()->set(row, val.asBool());
      break;
    case TypeKind::TINYINT:
      vec->asFlatVector<int8_t>()->set(
          row, static_cast<int8_t>(val.asInt()));
      break;
    case TypeKind::SMALLINT:
      vec->asFlatVector<int16_t>()->set(
          row, static_cast<int16_t>(val.asInt()));
      break;
    case TypeKind::INTEGER:
      vec->asFlatVector<int32_t>()->set(
          row, static_cast<int32_t>(val.asInt()));
      break;
    case TypeKind::BIGINT:
      vec->asFlatVector<int64_t>()->set(row, val.asInt());
      break;
    case TypeKind::REAL:
      vec->asFlatVector<float>()->set(
          row, static_cast<float>(val.asDouble()));
      break;
    case TypeKind::DOUBLE:
      vec->asFlatVector<double>()->set(row, val.asDouble());
      break;
    case TypeKind::VARCHAR: {
      auto str = val.asString();
      setStringValue(vec->asFlatVector<StringView>(), row, str);
      break;
    }
    case TypeKind::VARBINARY: {
      auto encoded = val.asString();
      auto decoded = encoding::Base64::decode(encoded);
      setStringValue(vec->asFlatVector<StringView>(), row, decoded);
      break;
    }
    case TypeKind::TIMESTAMP: {
      auto str = val.asString();
      auto tsResult =
          util::fromTimestampString(StringView(str), util::TimestampParseMode::kPrestoCast);
      VELOX_USER_CHECK(tsResult.hasValue(), "Invalid timestamp string: {}", str);
      vec->asFlatVector<Timestamp>()->set(row, tsResult.value());
      break;
    }
    case TypeKind::ARRAY: {
      VELOX_USER_CHECK(
          val.isArray(),
          "Expected JSON array for ARRAY column, got: {}",
          folly::toJson(val));
      auto* arrVec = vec->as<ArrayVector>();
      auto& elements = arrVec->elements();
      const auto offset = static_cast<vector_size_t>(elements->size());
      const auto arraySize = static_cast<vector_size_t>(val.size());
      elements->resize(offset + arraySize);
      arrVec->offsets()->asMutable<vector_size_t>()[row] = offset;
      arrVec->sizes()->asMutable<vector_size_t>()[row] = arraySize;
      const auto& elementType = type->asArray().elementType();
      for (vector_size_t i = 0; i < arraySize; ++i) {
        setFieldValue(val[i], elementType, elements.get(), offset + i);
      }
      break;
    }
    case TypeKind::MAP: {
      VELOX_USER_CHECK(
          val.isObject(),
          "Expected JSON object for MAP column, got: {}",
          folly::toJson(val));
      auto* mapVec = vec->as<MapVector>();
      auto& keys = mapVec->mapKeys();
      auto& values = mapVec->mapValues();
      const auto offset = static_cast<vector_size_t>(keys->size());
      const auto mapSize = static_cast<vector_size_t>(val.size());
      keys->resize(offset + mapSize);
      values->resize(offset + mapSize);
      mapVec->offsets()->asMutable<vector_size_t>()[row] = offset;
      mapVec->sizes()->asMutable<vector_size_t>()[row] = mapSize;
      const auto& keyType = type->asMap().keyType();
      const auto& valueType = type->asMap().valueType();
      vector_size_t idx = 0;
      for (const auto& [k, v] : val.items()) {
        folly::dynamic keyDyn(k);
        setFieldValue(keyDyn, keyType, keys.get(), offset + idx);
        setFieldValue(v, valueType, values.get(), offset + idx);
        ++idx;
      }
      break;
    }
    case TypeKind::ROW: {
      VELOX_USER_CHECK(
          val.isObject(),
          "Expected JSON object for ROW column, got: {}",
          folly::toJson(val));
      auto* rowVec = vec->as<RowVector>();
      const auto& rowType = type->asRow();
      for (size_t i = 0; i < rowType.size(); ++i) {
        const auto& fieldName = rowType.nameOf(i);
        auto* childVec = rowVec->childAt(i).get();
        if (val.count(fieldName) == 0) {
          childVec->setNull(row, true);
        } else {
          setFieldValue(val[fieldName], rowType.childAt(i), childVec, row);
        }
      }
      break;
    }
    default:
      VELOX_NYI("JSON reader does not support type {}", type->toString());
  }
}

int64_t JsonRowReader::nextRowNumber() {
  return atEOF_ ? dwio::common::RowReader::kAtEnd
                : static_cast<int64_t>(currentRow_);
}

int64_t JsonRowReader::nextReadSize(uint64_t size) {
  return atEOF_ ? dwio::common::RowReader::kAtEnd
                : static_cast<int64_t>(size);
}

void JsonRowReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& /*stats*/) const {}

void JsonRowReader::resetFilterCaches() {}

std::optional<size_t> JsonRowReader::estimatedRowSize() const {
  return std::nullopt;
}

} // namespace facebook::velox::json
