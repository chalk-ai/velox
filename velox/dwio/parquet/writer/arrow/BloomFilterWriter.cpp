/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Adapted from Apache Arrow (parquet/bloom_filter_writer.cc).

#include "velox/dwio/parquet/writer/arrow/BloomFilterWriter.h"

#include <array>
#include <limits>
#include <map>
#include <sstream>
#include <utility>

#include "arrow/array.h"
#include "arrow/io/interfaces.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/checked_cast.h"

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/parquet/writer/arrow/Exception.h"
#include "velox/dwio/parquet/writer/arrow/Properties.h"
#include "velox/dwio/parquet/writer/arrow/Schema.h"
#include "velox/dwio/parquet/writer/arrow/Types.h"

namespace facebook::velox::parquet::arrow {

constexpr int64_t kHashBatchSize = 256;

template <typename ParquetType>
TypedBloomFilterWriter<ParquetType>::TypedBloomFilterWriter(
    const ColumnDescriptor* descr,
    BloomFilter* bloomFilter)
    : descr_(descr), bloomFilter_(bloomFilter) {}

template <typename ParquetType>
void TypedBloomFilterWriter<ParquetType>::update(
    const T* values,
    int64_t numValues) {
  VELOX_DCHECK_NOT_NULL(bloomFilter_);
  std::array<uint64_t, kHashBatchSize> hashes;
  for (int64_t i = 0; i < numValues; i += kHashBatchSize) {
    auto batchSize = static_cast<int>(std::min(kHashBatchSize, numValues - i));
    if constexpr (std::is_same_v<ParquetType, FLBAType>) {
      bloomFilter_->hashes(
          values + i, descr_->typeLength(), batchSize, hashes.data());
    } else {
      bloomFilter_->hashes(values + i, batchSize, hashes.data());
    }
    bloomFilter_->insertHashes(hashes.data(), batchSize);
  }
}

template <>
void TypedBloomFilterWriter<BooleanType>::update(const bool*, int64_t) {
  throw ParquetException("Bloom filter is not supported for boolean type");
}

template <typename ParquetType>
void TypedBloomFilterWriter<ParquetType>::updateSpaced(
    const T* values,
    int64_t numValues,
    const uint8_t* validBits,
    int64_t validBitsOffset) {
  VELOX_DCHECK_NOT_NULL(bloomFilter_);
  std::array<uint64_t, kHashBatchSize> hashes;
  ::arrow::internal::VisitSetBitRunsVoid(
      validBits,
      validBitsOffset,
      numValues,
      [&](int64_t position, int64_t length) {
        for (int64_t i = 0; i < length; i += kHashBatchSize) {
          auto batchSize =
              static_cast<int>(std::min(kHashBatchSize, length - i));
          if constexpr (std::is_same_v<ParquetType, FLBAType>) {
            bloomFilter_->hashes(
                values + i + position,
                descr_->typeLength(),
                batchSize,
                hashes.data());
          } else {
            bloomFilter_->hashes(
                values + i + position, batchSize, hashes.data());
          }
          bloomFilter_->insertHashes(hashes.data(), batchSize);
        }
      });
}

template <>
void TypedBloomFilterWriter<BooleanType>::updateSpaced(
    const bool*,
    int64_t,
    const uint8_t*,
    int64_t) {
  throw ParquetException("Bloom filter is not supported for boolean type");
}

template <typename ParquetType>
void TypedBloomFilterWriter<ParquetType>::update(const ::arrow::Array& values) {
  ParquetException::NYI(
      "Updating bloom filter is not implemented for array of type: " +
      values.type()->ToString());
}

namespace {

template <typename ArrayType>
void updateBinaryBloomFilter(BloomFilter& bloomFilter, const ArrayType& array) {
  std::array<ByteArray, kHashBatchSize> byteArrays;
  std::array<uint64_t, kHashBatchSize> hashes;
  ::arrow::internal::VisitSetBitRunsVoid(
      array.null_bitmap_data(),
      array.offset(),
      array.length(),
      [&](int64_t position, int64_t length) {
        for (int64_t i = 0; i < length; i += kHashBatchSize) {
          auto batchSize =
              static_cast<int>(std::min(kHashBatchSize, length - i));
          for (int j = 0; j < batchSize; j++) {
            byteArrays[j] = array.GetView(position + i + j);
          }
          bloomFilter.hashes(byteArrays.data(), batchSize, hashes.data());
          bloomFilter.insertHashes(hashes.data(), batchSize);
        }
      });
}

} // namespace

template <>
void TypedBloomFilterWriter<ByteArrayType>::update(
    const ::arrow::Array& values) {
  VELOX_DCHECK_NOT_NULL(bloomFilter_);
  if (::arrow::is_binary_like(values.type_id())) {
    updateBinaryBloomFilter(
        *bloomFilter_,
        ::arrow::internal::checked_cast<const ::arrow::BinaryArray&>(values));
  } else if (::arrow::is_large_binary_like(values.type_id())) {
    updateBinaryBloomFilter(
        *bloomFilter_,
        ::arrow::internal::checked_cast<const ::arrow::LargeBinaryArray&>(
            values));
  } else {
    ParquetException::NYI(
        "Bloom filter is not supported for this Arrow type: " +
        values.type()->ToString());
  }
}

template class TypedBloomFilterWriter<BooleanType>;
template class TypedBloomFilterWriter<Int32Type>;
template class TypedBloomFilterWriter<Int64Type>;
template class TypedBloomFilterWriter<Int96Type>;
template class TypedBloomFilterWriter<FloatType>;
template class TypedBloomFilterWriter<DoubleType>;
template class TypedBloomFilterWriter<ByteArrayType>;
template class TypedBloomFilterWriter<FLBAType>;

namespace {

/// A concrete implementation of BloomFilterBuilder.
///
/// \note Column encryption for bloom filters is not implemented.
class BloomFilterBuilderImpl : public BloomFilterBuilder {
 public:
  BloomFilterBuilderImpl(
      const SchemaDescriptor* schema,
      const WriterProperties* properties)
      : schema_(schema), properties_(properties) {}

  void appendRowGroup() override;

  BloomFilter* createBloomFilter(int32_t columnOrdinal) override;

  BloomFilterLocation writeTo(::arrow::io::OutputStream* sink) override;

 private:
  /// Make sure the column ordinal is not out of bound and the builder is in a
  /// good state.
  void checkState(int32_t columnOrdinal) const {
    if (finished_) {
      throw ParquetException("BloomFilterBuilder is already finished.");
    }
    if (bloomFilters_.empty()) {
      throw ParquetException("No row group appended to BloomFilterBuilder");
    }
    if (columnOrdinal < 0 || columnOrdinal >= schema_->numColumns()) {
      throw ParquetException(
          "Invalid column ordinal: " + std::to_string(columnOrdinal));
    }
    if (schema_->column(columnOrdinal)->physicalType() == Type::kBoolean) {
      throw ParquetException(
          "BloomFilterBuilder does not support boolean type.");
    }
  }

  const SchemaDescriptor* schema_;
  const WriterProperties* properties_;
  bool finished_ = false;

  using RowGroupBloomFilters =
      std::map</*columnId=*/int32_t, std::shared_ptr<BloomFilter>>;
  // Indexed by row group ordinal.
  std::vector<RowGroupBloomFilters> bloomFilters_;
};

void BloomFilterBuilderImpl::appendRowGroup() {
  if (finished_) {
    throw ParquetException(
        "Cannot append a new row group to a finished BloomFilterBuilder");
  }
  bloomFilters_.emplace_back();
}

BloomFilter* BloomFilterBuilderImpl::createBloomFilter(int32_t columnOrdinal) {
  auto opts =
      properties_->bloomFilterOptions(schema_->column(columnOrdinal)->path());
  if (!opts.has_value()) {
    return nullptr;
  }

  checkState(columnOrdinal);

  auto& currRowGroup = *bloomFilters_.rbegin();
  if (currRowGroup.find(columnOrdinal) != currRowGroup.cend()) {
    std::stringstream ss;
    ss << "Bloom filter already exists for column: " << columnOrdinal
       << ", row group: " << (bloomFilters_.size() - 1);
    throw ParquetException(ss.str());
  }

  auto bloomFilter =
      std::make_unique<BlockSplitBloomFilter>(properties_->memoryPool());
  bloomFilter->init(
      BlockSplitBloomFilter::optimalNumOfBytes(opts->ndv, opts->fpp));
  return currRowGroup.emplace(columnOrdinal, std::move(bloomFilter))
      .first->second.get();
}

BloomFilterLocation BloomFilterBuilderImpl::writeTo(
    ::arrow::io::OutputStream* sink) {
  if (finished_) {
    throw ParquetException("Cannot write a finished BloomFilterBuilder");
  }
  finished_ = true;

  BloomFilterLocation location;

  for (size_t rowGroupOrdinal = 0; rowGroupOrdinal != bloomFilters_.size();
       ++rowGroupOrdinal) {
    const auto& rowGroupBloomFilters = bloomFilters_[rowGroupOrdinal];
    if (rowGroupBloomFilters.empty()) {
      continue;
    }
    auto& rowGroupLocation = location.bloomFilterLocation[rowGroupOrdinal];
    rowGroupLocation.resize(schema_->numColumns());
    for (const auto& [columnId, filter] : rowGroupBloomFilters) {
      PARQUET_ASSIGN_OR_THROW(int64_t offset, sink->Tell());
      filter->writeTo(sink);
      PARQUET_ASSIGN_OR_THROW(int64_t pos, sink->Tell());

      if (pos - offset > std::numeric_limits<int32_t>::max()) {
        throw ParquetException(
            "Bloom filter size is too large, size: " +
            std::to_string(pos - offset) +
            ", column: " + std::to_string(columnId) +
            ", row group: " + std::to_string(rowGroupOrdinal));
      }

      rowGroupLocation.at(columnId) =
          IndexLocation{offset, static_cast<int32_t>(pos - offset)};
    }
  }

  return location;
}

} // namespace

std::unique_ptr<BloomFilterBuilder> BloomFilterBuilder::make(
    const SchemaDescriptor* schema,
    const WriterProperties* properties) {
  return std::make_unique<BloomFilterBuilderImpl>(schema, properties);
}

} // namespace facebook::velox::parquet::arrow
