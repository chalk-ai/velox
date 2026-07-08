#pragma once

#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveSplitReader.h"

namespace facebook::velox::connector::hive::delta {

class DeltaSplitReader final : public HiveSplitReader {
 public:
  DeltaSplitReader(
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
      std::vector<column_index_t> bucketChannels = {},
      const common::SubfieldFilters* subfieldFiltersForValidation = nullptr);

  void prepareSplit(
      std::shared_ptr<common::MetadataFilter> metadataFilter,
      dwio::common::RuntimeStatistics& runtimeStats,
      const folly::F14FastMap<std::string, std::string>& fileReadOps = {})
      override;

  uint64_t next(uint64_t size, VectorPtr& output) override;

 private:
  uint64_t baseReadOffset_;
  uint64_t splitOffset_;
  std::vector<uint64_t> deletedRows_;
  size_t deletedRowsOffset_;
  BufferPtr deleteBitmap_;
};
} // namespace facebook::velox::connector::hive::delta
