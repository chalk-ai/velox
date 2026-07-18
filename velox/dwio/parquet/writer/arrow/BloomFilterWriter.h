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

// Adapted from Apache Arrow (parquet/bloom_filter_writer.h).

#pragma once

#include <memory>

#include "arrow/type_fwd.h"

#include "velox/dwio/parquet/writer/arrow/BloomFilter.h"
#include "velox/dwio/parquet/writer/arrow/Metadata.h"

namespace facebook::velox::parquet::arrow {

class ColumnDescriptor;
class SchemaDescriptor;
class WriterProperties;

/// \brief Writer for updating a bloom filter with values of a specific
/// Parquet type.
/// \note Boolean type is not supported.
template <typename ParquetType>
class PARQUET_EXPORT TypedBloomFilterWriter {
 public:
  using T = typename ParquetType::CType;

  /// \param descr The descriptor of the column to write. Must outlive this
  /// writer.
  /// \param bloomFilter The bloom filter to update. Must outlive this writer.
  TypedBloomFilterWriter(
      const ColumnDescriptor* descr,
      BloomFilter* bloomFilter);

  /// \brief Update the bloom filter with typed values.
  void update(const T* values, int64_t numValues);

  /// \brief Update the bloom filter with typed values that have spaces.
  void updateSpaced(
      const T* values,
      int64_t numValues,
      const uint8_t* validBits,
      int64_t validBitsOffset);

  /// \brief Update the bloom filter with an Arrow array (binary-like arrays
  /// written directly to a ByteArray column).
  void update(const ::arrow::Array& values);

 private:
  const ColumnDescriptor* descr_;
  BloomFilter* bloomFilter_;
};

/// \brief Interface for building the bloom filters of a parquet file.
class PARQUET_EXPORT BloomFilterBuilder {
 public:
  virtual ~BloomFilterBuilder() = default;

  /// \brief Create a BloomFilterBuilder.
  ///
  /// \param schema The schema of the file; must outlive the created builder.
  /// \param properties Properties carrying the per-column bloom filter
  /// options; must outlive the created builder.
  static std::unique_ptr<BloomFilterBuilder> make(
      const SchemaDescriptor* schema,
      const WriterProperties* properties);

  /// \brief Start a new row group: subsequent createBloomFilter calls create
  /// bloom filters for the new row group.
  virtual void appendRowGroup() = 0;

  /// \brief Create a BloomFilter for the column ordinal of the current row
  /// group, or return nullptr if bloom filters are not enabled for the
  /// column. Ownership stays with the builder.
  virtual BloomFilter* createBloomFilter(int32_t columnOrdinal) = 0;

  /// \brief Write all bloom filters to the sink and return their locations,
  /// keyed by row group ordinal with one optional entry per column. The bloom
  /// filters cannot be modified afterwards.
  virtual BloomFilterLocation writeTo(::arrow::io::OutputStream* sink) = 0;
};

} // namespace facebook::velox::parquet::arrow
