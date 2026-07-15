#include "velox/connectors/hive/delta/DeltaSplitReader.h"

#include <algorithm>
#include <cstring>
#include <utility>

#include "folly/json.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/BufferUtil.h"

namespace facebook::velox::connector::hive::delta {
namespace {
std::vector<uint64_t> parseDeltaDeletionVectorRowIndexes(
    const std::shared_ptr<std::string>& extraFileInfo) {
  if (!extraFileInfo || extraFileInfo->empty()) {
    return {};
  }

  folly::dynamic splitInfo;
  try {
    splitInfo = folly::parseJson(*extraFileInfo);
  } catch (const std::exception& e) {
    VELOX_USER_FAIL(
        "Failed to parse Delta split extraFileInfo JSON for deletion vector: {}",
        e.what());
  }

  VELOX_USER_CHECK(
      splitInfo.isObject(), "Delta split extraFileInfo must be a JSON object.");

  const auto* deletionVector = splitInfo.get_ptr("deletion_vector");
  if (deletionVector == nullptr || deletionVector->isNull()) {
    return {};
  }

  VELOX_USER_CHECK(
      deletionVector->isObject(),
      "Delta split 'deletion_vector' must be a JSON object when provided.");

  const auto* rowIndexes = deletionVector->get_ptr("rowIndexes");
  if (rowIndexes == nullptr || rowIndexes->isNull()) {
    return {};
  }

  VELOX_USER_CHECK(
      rowIndexes->isArray(),
      "Delta split 'deletion_vector.rowIndexes' must be a JSON array.");

  std::vector<uint64_t> parsedRows;
  parsedRows.reserve(rowIndexes->size());
  for (const auto& rowIndexValue : *rowIndexes) {
    VELOX_USER_CHECK(
        rowIndexValue.isInt(),
        "Delta split 'deletion_vector.rowIndexes' values must be integers.");
    const auto rowIndex = rowIndexValue.asInt();
    VELOX_USER_CHECK_GE(
        rowIndex,
        0,
        "Delta split 'deletion_vector.rowIndexes' values must be non-negative.");
    parsedRows.push_back(static_cast<uint64_t>(rowIndex));
  }

  std::sort(parsedRows.begin(), parsedRows.end());
  parsedRows.erase(
      std::unique(parsedRows.begin(), parsedRows.end()), parsedRows.end());
  return parsedRows;
}
} // namespace

DeltaSplitReader::DeltaSplitReader(
    const std::shared_ptr<const hive::HiveConnectorSplit>& hiveSplit,
    const FileTableHandlePtr& tableHandle,
    const std::unordered_map<std::string, FileColumnHandlePtr>* partitionKeys,
    const ConnectorQueryCtx* connectorQueryCtx,
    const std::shared_ptr<const FileConfig>& fileConfig,
    const RowTypePtr& readerOutputType,
    const std::shared_ptr<io::IoStatistics>& dataIoStats,
    const std::shared_ptr<io::IoStatistics>& metadataIoStats,
    const std::shared_ptr<IoStats>& ioStats,
    FileHandleFactory* fileHandleFactory,
    folly::Executor* ioExecutor,
    const std::shared_ptr<common::ScanSpec>& scanSpec,
    const std::unordered_map<std::string, FileColumnHandlePtr>* infoColumns,
    std::vector<column_index_t> bucketChannels,
    const common::SubfieldFilters* subfieldFiltersForValidation)
    : HiveSplitReader(
          hiveSplit,
          tableHandle,
          partitionKeys,
          connectorQueryCtx,
          fileConfig,
          readerOutputType,
          dataIoStats,
          metadataIoStats,
          ioStats,
          fileHandleFactory,
          ioExecutor,
          scanSpec,
          infoColumns,
          std::move(bucketChannels),
          subfieldFiltersForValidation),
      baseReadOffset_(0),
      splitOffset_(0),
      deletedRows_(),
      deletedRowsOffset_(0),
      deleteBitmap_(nullptr) {}

void DeltaSplitReader::prepareSplit(
    std::shared_ptr<common::MetadataFilter> metadataFilter,
    dwio::common::RuntimeStatistics& runtimeStats,
    const folly::F14FastMap<std::string, std::string>& fileReadOps) {
  HiveSplitReader::prepareSplit(
      std::move(metadataFilter), runtimeStats, fileReadOps);
  if (emptySplit_) {
    return;
  }
  baseReadOffset_ = 0;
  splitOffset_ = baseRowReader_->nextRowNumber();
  deletedRows_ = parseDeltaDeletionVectorRowIndexes(hiveSplit_->extraFileInfo);
  deletedRowsOffset_ = 0;
}

uint64_t DeltaSplitReader::next(uint64_t size, VectorPtr& output) {
  dwio::common::Mutation mutation;
  mutation.randomSkip = baseReaderOpts_.randomSkip().get();
  mutation.deletedRows = nullptr;

  if (deleteBitmap_) {
    deleteBitmap_->setSize(0);
  }

  const auto actualSize = baseRowReader_->nextReadSize(size);
  baseReadOffset_ = baseRowReader_->nextRowNumber() - splitOffset_;
  if (actualSize == dwio::common::RowReader::kAtEnd) {
    return 0;
  }

  if (!deletedRows_.empty() && deletedRowsOffset_ < deletedRows_.size()) {
    const auto numBytes = bits::nbytes(actualSize);
    dwio::common::ensureCapacity<int8_t>(
        deleteBitmap_, numBytes, connectorQueryCtx_->memoryPool(), false, true);
    std::memset(
        static_cast<void*>(deleteBitmap_->asMutable<int8_t>()), 0L, numBytes);

    const auto rowNumberLowerBound = splitOffset_ + baseReadOffset_;
    const auto rowNumberUpperBound = rowNumberLowerBound + actualSize;
    auto* deleteBitmap = deleteBitmap_->asMutable<uint8_t>();
    bool hasDeletesInBatch = false;

    while (deletedRowsOffset_ < deletedRows_.size() &&
           deletedRows_[deletedRowsOffset_] < rowNumberLowerBound) {
      ++deletedRowsOffset_;
    }

    while (deletedRowsOffset_ < deletedRows_.size() &&
           deletedRows_[deletedRowsOffset_] < rowNumberUpperBound) {
      bits::setBit(
          deleteBitmap, deletedRows_[deletedRowsOffset_] - rowNumberLowerBound);
      hasDeletesInBatch = true;
      ++deletedRowsOffset_;
    }

    deleteBitmap_->setSize(hasDeletesInBatch ? numBytes : 0);
  }

  mutation.deletedRows = deleteBitmap_ && deleteBitmap_->size() > 0
      ? deleteBitmap_->as<uint64_t>()
      : nullptr;

  auto numScanned = baseRowReader_->next(actualSize, output, &mutation);
  if (numScanned > 0 && output->size() > 0 && !bucketChannels().empty()) {
    applyBucketConversion(
        output, bucketConversionRows(*output->asChecked<RowVector>()));
  }
  return numScanned;
}
} // namespace facebook::velox::connector::hive::delta
