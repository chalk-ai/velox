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

#include "velox/dwio/json/writer/JsonWriter.h"

#include <folly/Conv.h>
#include <folly/Random.h>
#include <folly/json.h>

#include "velox/common/encode/Base64.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::json {

namespace {

folly::json::serialization_opts jsonOpts() {
  folly::json::serialization_opts opts;
  // Standard JSON does not support NaN or Infinity; replace with null.
  opts.allow_nan_inf = false;
  opts.encode_non_ascii = false;
  return opts;
}

} // namespace

JsonWriter::JsonWriter(
    RowTypePtr schema,
    std::unique_ptr<dwio::common::FileSink> sink,
    const std::shared_ptr<WriterOptions>& options)
    : schema_(std::move(schema)),
      sink_(std::move(sink)),
      pool_(options->memoryPool->addLeafChild(fmt::format(
          "{}.json_writer_node.{}",
          options->memoryPool->name(),
          folly::to<std::string>(folly::Random::rand64())))),
      flushThresholdBytes_(options->flushThresholdBytes) {
  setState(State::kRunning);
}

void JsonWriter::write(const VectorPtr& data) {
  checkRunning();
  VELOX_CHECK_EQ(
      data->encoding(),
      VectorEncoding::Simple::ROW,
      "JSON writer expects a RowVector");

  const auto* rowVec = data->as<RowVector>();
  const auto numRows = rowVec->size();
  const auto numCols = schema_->size();

  std::vector<DecodedVector> decoded(numCols);
  SelectivityVector rows(numRows);
  for (size_t col = 0; col < numCols; ++col) {
    decoded[col].decode(*rowVec->childAt(col), rows);
  }

  static const auto opts = jsonOpts();

  for (vector_size_t row = 0; row < numRows; ++row) {
    folly::dynamic obj = folly::dynamic::object();
    for (size_t col = 0; col < numCols; ++col) {
      obj[schema_->nameOf(col)] =
          vectorToJson(decoded[col], schema_->childAt(col), row);
    }
    buffer_ += folly::json::serialize(obj, opts);
    buffer_ += '\n';
  }

  if (buffer_.size() >= flushThresholdBytes_) {
    flushBuffer();
  }
}

folly::dynamic JsonWriter::vectorToJson(
    const DecodedVector& decoded,
    const TypePtr& type,
    vector_size_t row) {
  if (decoded.isNullAt(row)) {
    return folly::dynamic(nullptr);
  }

  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return folly::dynamic(decoded.valueAt<bool>(row));
    case TypeKind::TINYINT:
      return folly::dynamic(static_cast<int64_t>(decoded.valueAt<int8_t>(row)));
    case TypeKind::SMALLINT:
      return folly::dynamic(
          static_cast<int64_t>(decoded.valueAt<int16_t>(row)));
    case TypeKind::INTEGER:
      return folly::dynamic(
          static_cast<int64_t>(decoded.valueAt<int32_t>(row)));
    case TypeKind::BIGINT:
      return folly::dynamic(decoded.valueAt<int64_t>(row));
    case TypeKind::REAL: {
      const float v = decoded.valueAt<float>(row);
      if (std::isnan(v) || std::isinf(v)) {
        return folly::dynamic(nullptr);
      }
      return folly::dynamic(static_cast<double>(v));
    }
    case TypeKind::DOUBLE: {
      const double v = decoded.valueAt<double>(row);
      if (std::isnan(v) || std::isinf(v)) {
        return folly::dynamic(nullptr);
      }
      return folly::dynamic(v);
    }
    case TypeKind::VARCHAR: {
      const auto sv = decoded.valueAt<StringView>(row);
      return folly::dynamic(std::string(sv.data(), sv.size()));
    }
    case TypeKind::VARBINARY: {
      const auto sv = decoded.valueAt<StringView>(row);
      return folly::dynamic(
          encoding::Base64::encode(sv.data(), sv.size()));
    }
    case TypeKind::TIMESTAMP: {
      const auto ts = decoded.valueAt<Timestamp>(row);
      TimestampToStringOptions opts;
      opts.precision = TimestampPrecision::kMilliseconds;
      return folly::dynamic(ts.toString(opts));
    }
    case TypeKind::ARRAY: {
      const auto* arrVec = decoded.base()->as<ArrayVector>();
      const auto idx = decoded.indices()[row];
      const auto offset = arrVec->offsetAt(idx);
      const auto size = arrVec->sizeAt(idx);
      auto elements = arrVec->elements();
      DecodedVector decodedElem(*elements);
      folly::dynamic arr = folly::dynamic::array();
      for (vector_size_t i = 0; i < size; ++i) {
        arr.push_back(
            vectorToJson(decodedElem, type->asArray().elementType(), offset + i));
      }
      return arr;
    }
    case TypeKind::MAP: {
      const auto* mapVec = decoded.base()->as<MapVector>();
      const auto idx = decoded.indices()[row];
      const auto offset = mapVec->offsetAt(idx);
      const auto size = mapVec->sizeAt(idx);
      DecodedVector decodedKeys(*mapVec->mapKeys());
      DecodedVector decodedValues(*mapVec->mapValues());
      folly::dynamic obj = folly::dynamic::object();
      const auto& keyType = type->asMap().keyType();
      const auto& valueType = type->asMap().valueType();
      for (vector_size_t i = 0; i < size; ++i) {
        auto keyDyn = vectorToJson(decodedKeys, keyType, offset + i);
        VELOX_CHECK(
            keyDyn.isString(),
            "JSON writer requires MAP keys to be strings");
        obj[keyDyn.asString()] =
            vectorToJson(decodedValues, valueType, offset + i);
      }
      return obj;
    }
    case TypeKind::ROW: {
      const auto* rowVec = decoded.base()->as<RowVector>();
      const auto idx = decoded.indices()[row];
      const auto& rowType = type->asRow();
      folly::dynamic obj = folly::dynamic::object();
      for (size_t i = 0; i < rowType.size(); ++i) {
        DecodedVector decodedChild(*rowVec->childAt(i));
        obj[rowType.nameOf(i)] =
            vectorToJson(decodedChild, rowType.childAt(i), idx);
      }
      return obj;
    }
    default:
      VELOX_NYI("JSON writer does not support type {}", type->toString());
  }
}

void JsonWriter::flushBuffer() {
  if (buffer_.empty()) {
    return;
  }
  dwio::common::DataBuffer<char> buf(*pool_.get(), buffer_.size());
  memcpy(buf.data(), buffer_.data(), buffer_.size());
  sink_->write(std::move(buf));
  buffer_.clear();
}

void JsonWriter::flush() {
  flushBuffer();
}

void JsonWriter::close() {
  if (state() == State::kClosed || state() == State::kAborted) {
    return;
  }
  flushBuffer();
  sink_->close();
  setState(State::kClosed);
}

void JsonWriter::abort() {
  if (state() == State::kClosed || state() == State::kAborted) {
    return;
  }
  buffer_.clear();
  sink_->close();
  setState(State::kAborted);
}

std::unique_ptr<dwio::common::Writer> JsonWriterFactory::createWriter(
    std::unique_ptr<dwio::common::FileSink> sink,
    const std::shared_ptr<dwio::common::WriterOptions>& options) {
  auto jsonOptions = std::dynamic_pointer_cast<WriterOptions>(options);
  VELOX_CHECK_NOT_NULL(
      jsonOptions, "JSON writer factory expected a JSON WriterOptions object.");
  return std::make_unique<JsonWriter>(
      asRowType(options->schema), std::move(sink), jsonOptions);
}

std::unique_ptr<dwio::common::WriterOptions>
JsonWriterFactory::createWriterOptions() {
  return std::make_unique<WriterOptions>();
}

} // namespace facebook::velox::json
