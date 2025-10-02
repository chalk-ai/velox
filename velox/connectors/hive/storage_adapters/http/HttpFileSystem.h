#pragma once

#include "velox/common/file/FileSystems.h"

namespace facebook::velox::filesystems {

/// Initializes the global curl state required by HttpFileSystem. Safe to call
/// multiple times; initialization happens once.
bool initializeHttp(
    std::string_view logLevel = "FATAL",
    std::optional<std::string_view> logLocation = std::nullopt);

/// Releases global curl state. Safe to call without a preceding initialize.
void finalizeHttp();

/// Lightweight read-only filesystem backed by HTTP GET requests.
class HttpFileSystem : public FileSystem {
 public:
  explicit HttpFileSystem(std::shared_ptr<const config::ConfigBase> config);

  std::string name() const override;

  std::string_view extractPath(std::string_view path) const override;

  std::unique_ptr<ReadFile> openFileForRead(
      std::string_view path,
      const FileOptions& options = {}) override;

  std::unique_ptr<WriteFile> openFileForWrite(
      std::string_view path,
      const FileOptions& options = {}) override;

  void remove(std::string_view path) override;

  void rename(
      std::string_view source,
      std::string_view destination,
      bool overwrite = false) override;

  bool exists(std::string_view path) override;

  std::vector<std::string> list(std::string_view path) override;

  void mkdir(std::string_view path, const DirectoryOptions& options = {})
      override;

  void rmdir(std::string_view path) override;

};

} // namespace facebook::velox::filesystems
