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

#include "velox/dwio/parquet/reader/ParquetData.h"

#include <limits>
#include <optional>
#include <string_view>

#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/parquet/common/BloomFilter.h"
#include "velox/dwio/parquet/common/XxHasher.h"
#include "velox/dwio/parquet/reader/ParquetStatsContext.h"
#include "velox/type/Filter.h"

namespace facebook::velox::parquet {

std::unique_ptr<dwio::common::FormatData> ParquetParams::toFormatData(
    const std::shared_ptr<const dwio::common::TypeWithId>& type,
    const common::ScanSpec& /*scanSpec*/) {
  return std::make_unique<ParquetData>(
      type,
      metaData_,
      pool(),
      runtimeStatistics(),
      sessionTimezone_,
      bufferedInput_);
}

void ParquetData::filterRowGroups(
    const common::ScanSpec& scanSpec,
    uint64_t /*rowsPerRowGroup*/,
    const dwio::common::StatsContext& writerContext,
    FilterRowGroupsResult& result) {
  auto parquetStatsContext =
      reinterpret_cast<const ParquetStatsContext*>(&writerContext);
  if (type_->parquetType_.has_value() &&
      parquetStatsContext->shouldIgnoreStatistics(
          type_->parquetType_.value())) {
    return;
  }
  result.totalCount =
      std::max<int>(result.totalCount, fileMetaDataPtr_.numRowGroups());
  auto nwords = bits::nwords(result.totalCount);
  if (result.filterResult.size() < nwords) {
    result.filterResult.resize(nwords);
  }
  auto metadataFiltersStartIndex = result.metadataFilterResults.size();
  for (int i = 0; i < scanSpec.numMetadataFilters(); ++i) {
    result.metadataFilterResults.emplace_back(
        scanSpec.metadataFilterNodeAt(i), std::vector<uint64_t>(nwords));
  }
  if (scanSpec.filter() || scanSpec.numMetadataFilters() > 0) {
    for (auto i = 0; i < fileMetaDataPtr_.numRowGroups(); ++i) {
      // Already excluded by another column or by the caller (e.g. row group
      // outside the split range, empty row group). Skip statistics build and
      // testFilter. The MetadataFilter::eval call ORs into filterResult, so
      // leaving the per-leaf metadata bits at 0 here is harmless: filterResult
      // already has the bit set.
      if (bits::isBitSet(result.filterResult.data(), i)) {
        continue;
      }
      if (scanSpec.filter() && !rowGroupMatches(i, scanSpec.filter())) {
        bits::setBit(result.filterResult.data(), i);
        continue;
      }
      for (int j = 0; j < scanSpec.numMetadataFilters(); ++j) {
        auto* metadataFilter = scanSpec.metadataFilterAt(j);
        if (!rowGroupMatches(i, metadataFilter)) {
          bits::setBit(
              result.metadataFilterResults[metadataFiltersStartIndex + j]
                  .second.data(),
              i);
        }
      }
    }
  }
}

bool ParquetData::rowGroupMatches(
    uint32_t rowGroupId,
    const common::Filter* filter) {
  auto column = type_->column();
  auto type = type_->type();
  auto rowGroup = fileMetaDataPtr_.rowGroup(rowGroupId);
  assert(rowGroup.numColumns() != 0);

  if (!filter) {
    return true;
  }

  auto columnChunk = rowGroup.columnChunk(column);
  if (columnChunk.hasStatistics()) {
    auto columnStats =
        columnChunk.getColumnStatistics(type, rowGroup.numRows());
    if (!testFilter(filter, columnStats.get(), rowGroup.numRows(), type)) {
      return false;
    }
  }
  return rowGroupBloomFilterMightMatch(rowGroupId, filter);
}

namespace {

// Hashes of the values a point/IN equality filter accepts, computed with the
// parquet bloom hash of the column's physical type. std::nullopt when the
// filter shape or physical type is unsupported for bloom probing. An empty
// vector means none of the accepted values are representable in the column's
// physical type, so the filter cannot match at all.
std::optional<std::vector<uint64_t>> bloomProbeHashes(
    const common::Filter& filter,
    thrift::Type physicalType) {
  // A bloom filter can only prove that a value is absent. A filter that
  // accepts nulls may pass on a row group whose matching rows are all null,
  // so it cannot be used to prune.
  if (filter.testNull()) {
    return std::nullopt;
  }

  std::vector<int64_t> intValues;
  std::vector<std::string_view> byteValues;
  switch (filter.kind()) {
    case common::FilterKind::kBigintRange: {
      const auto& range = static_cast<const common::BigintRange&>(filter);
      if (!range.isSingleValue()) {
        return std::nullopt;
      }
      intValues.push_back(range.lower());
      break;
    }
    case common::FilterKind::kBigintValuesUsingHashTable: {
      intValues = static_cast<const common::BigintValuesUsingHashTable&>(filter)
                      .values();
      break;
    }
    case common::FilterKind::kBigintValuesUsingBitmask: {
      intValues =
          static_cast<const common::BigintValuesUsingBitmask&>(filter).values();
      break;
    }
    case common::FilterKind::kBytesRange: {
      const auto& range = static_cast<const common::BytesRange&>(filter);
      if (!range.isSingleValue()) {
        return std::nullopt;
      }
      byteValues.push_back(range.lower());
      break;
    }
    case common::FilterKind::kBytesValues: {
      const auto& values =
          static_cast<const common::BytesValues&>(filter).values();
      byteValues.reserve(values.size());
      for (const auto& value : values) {
        byteValues.push_back(value);
      }
      break;
    }
    default:
      return std::nullopt;
  }

  XxHasher hasher;
  std::vector<uint64_t> hashes;
  if (!byteValues.empty()) {
    if (physicalType != thrift::Type::BYTE_ARRAY) {
      return std::nullopt;
    }
    hashes.reserve(byteValues.size());
    for (const auto& value : byteValues) {
      ByteArray byteArray{
          static_cast<uint32_t>(value.size()),
          reinterpret_cast<const uint8_t*>(value.data())};
      hashes.push_back(hasher.hash(&byteArray));
    }
    return hashes;
  }

  switch (physicalType) {
    case thrift::Type::INT32:
      hashes.reserve(intValues.size());
      for (auto value : intValues) {
        // Values outside int32 cannot occur in an INT32 column; they
        // contribute no probe (and no possible match).
        if (value >= std::numeric_limits<int32_t>::min() &&
            value <= std::numeric_limits<int32_t>::max()) {
          hashes.push_back(hasher.hash(static_cast<int32_t>(value)));
        }
      }
      return hashes;
    case thrift::Type::INT64:
      hashes.reserve(intValues.size());
      for (auto value : intValues) {
        hashes.push_back(hasher.hash(value));
      }
      return hashes;
    default:
      return std::nullopt;
  }
}

} // namespace

bool ParquetData::rowGroupBloomFilterMightMatch(
    uint32_t rowGroupId,
    const common::Filter* filter) {
  if (filter == nullptr || bufferedInput_ == nullptr ||
      !type_->parquetType_.has_value()) {
    return true;
  }
  auto columnChunk =
      fileMetaDataPtr_.rowGroup(rowGroupId).columnChunk(type_->column());
  if (!columnChunk.hasBloomFilterOffset()) {
    return true;
  }
  const auto hashes = bloomProbeHashes(*filter, type_->parquetType_.value());
  if (!hashes.has_value()) {
    return true;
  }
  try {
    const auto offset = columnChunk.bloomFilterOffset();
    const auto fileSize =
        static_cast<int64_t>(bufferedInput_->getReadFile()->size());
    if (offset <= 0 || offset >= fileSize) {
      return true;
    }
    // Older writers do not record the length; bound the stream by end of file
    // and let the deserializer stop after the header-declared bitset size.
    const auto length = columnChunk.hasBloomFilterLength()
        ? static_cast<uint64_t>(columnChunk.bloomFilterLength())
        : static_cast<uint64_t>(fileSize - offset);
    dwio::common::SeekableFileInputStream stream(
        bufferedInput_->getInputStream(),
        static_cast<uint64_t>(offset),
        length,
        pool_,
        dwio::common::LogType::FILE);
    const auto bloomFilter = BlockSplitBloomFilter::deserialize(&stream, pool_);
    for (const auto hash : *hashes) {
      if (bloomFilter.findHash(hash)) {
        return true;
      }
    }
    return false;
  } catch (const std::exception& e) {
    // Bloom filter probing is a best-effort pruning optimization; a
    // malformed filter must not fail the scan.
    LOG(WARNING) << "Failed to probe parquet bloom filter: " << e.what();
    return true;
  }
}

void ParquetData::enqueueRowGroup(
    uint32_t index,
    dwio::common::BufferedInput& input) {
  auto chunk = fileMetaDataPtr_.rowGroup(index).columnChunk(type_->column());
  streams_.resize(fileMetaDataPtr_.numRowGroups());
  VELOX_CHECK(
      chunk.hasMetadata(),
      "ColumnMetaData does not exist for schema Id ",
      type_->column());
  ;

  uint64_t chunkReadOffset = chunk.dataPageOffset();
  if (chunk.hasDictionaryPageOffset() && chunk.dictionaryPageOffset() >= 4) {
    // this assumes the data pages follow the dict pages directly.
    chunkReadOffset = chunk.dictionaryPageOffset();
  }

  uint64_t readSize =
      (chunk.compression() == common::CompressionKind::CompressionKind_NONE)
      ? chunk.totalUncompressedSize()
      : chunk.totalCompressedSize();

  auto id = dwio::common::StreamIdentifier(type_->column());
  streams_[index] = input.enqueue({chunkReadOffset, readSize}, &id);
}

dwio::common::PositionProvider ParquetData::seekToRowGroup(int64_t index) {
  static std::vector<uint64_t> empty;
  VELOX_CHECK_LT(index, streams_.size());
  VELOX_CHECK(streams_[index], "Stream not enqueued for column");
  auto metadata = fileMetaDataPtr_.rowGroup(index).columnChunk(type_->column());
  reader_ = std::make_unique<PageReader>(
      std::move(streams_[index]),
      pool_,
      type_,
      metadata.compression(),
      metadata.totalCompressedSize(),
      stats_,
      sessionTimezone_);
  return dwio::common::PositionProvider(empty);
}

std::pair<int64_t, int64_t> ParquetData::getRowGroupRegion(
    uint32_t index) const {
  auto rowGroup = fileMetaDataPtr_.rowGroup(index);

  VELOX_CHECK_GT(rowGroup.numColumns(), 0);
  auto fileOffset = (rowGroup.hasFileOffset() && rowGroup.fileOffset() != 0)
      ? rowGroup.fileOffset()
      : rowGroup.columnChunk(0).hasDictionaryPageOffset()
      ? rowGroup.columnChunk(0).dictionaryPageOffset()
      : rowGroup.columnChunk(0).dataPageOffset();
  VELOX_CHECK_GT(fileOffset, 0);

  auto length = rowGroup.hasTotalCompressedSize()
      ? rowGroup.totalCompressedSize()
      : rowGroup.totalByteSize();

  return {fileOffset, length};
}

} // namespace facebook::velox::parquet
