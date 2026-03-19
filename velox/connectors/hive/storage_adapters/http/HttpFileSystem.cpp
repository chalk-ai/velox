#include "velox/connectors/hive/storage_adapters/http/HttpFileSystem.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"

#include <curl/curl.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <folly/Range.h>
#include <folly/ScopeGuard.h>

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <memory>
#include <sys/stat.h>
#include <unistd.h>

namespace facebook::velox::filesystems {

namespace {

constexpr std::string_view kHttpScheme{"http://"};
constexpr std::string_view kHttpsScheme{"https://"};
constexpr long kDefaultMaxRedirects{5};

class CurlEasyHandle {
 public:
  CurlEasyHandle() : handle_(curl_easy_init()) {
    VELOX_CHECK_NOT_NULL(handle_, "Failed to allocate CURL easy handle");
  }

  ~CurlEasyHandle() {
    if (handle_ != nullptr) {
      curl_easy_cleanup(handle_);
    }
  }

  CurlEasyHandle(const CurlEasyHandle&) = delete;
  CurlEasyHandle& operator=(const CurlEasyHandle&) = delete;

  CurlEasyHandle(CurlEasyHandle&& other) noexcept : handle_(other.handle_) {
    other.handle_ = nullptr;
  }

  CurlEasyHandle& operator=(CurlEasyHandle&& other) noexcept {
    if (this != &other) {
      if (handle_ != nullptr) {
        curl_easy_cleanup(handle_);
      }
      handle_ = other.handle_;
      other.handle_ = nullptr;
    }
    return *this;
  }

  CURL* get() const {
    return handle_;
  }

 private:
  CURL* handle_{nullptr};
};

void applyCommonCurlOptions(CURL* handle) {
  curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 1L);
  curl_easy_setopt(handle, CURLOPT_MAXREDIRS, kDefaultMaxRedirects);
  curl_easy_setopt(handle, CURLOPT_USERAGENT, "velox-httpfs/1.0");
  curl_easy_setopt(handle, CURLOPT_NOPROGRESS, 1L);
  curl_easy_setopt(handle, CURLOPT_NOSIGNAL, 1L);
}

enum class HeadProbeStatus { kSuccess, kNotFound, kInconclusive };

struct HeadProbeResult {
  HeadProbeStatus status;
  std::optional<int64_t> contentLength;
};

std::string getTempDirectory() {
  if (const char* tmpDir = std::getenv("TMPDIR")) {
    return std::string(tmpDir);
  }
  return "/tmp";
}

std::pair<std::string, int> createTemporaryFile() {
  auto basePath = getTempDirectory();
  auto pattern = fmt::format("{}/velox_httpfs_XXXXXX", basePath);
  std::vector<char> storage(pattern.begin(), pattern.end());
  storage.push_back('\0');
  int fd = mkstemp(storage.data());
  if (fd == -1) {
    VELOX_FAIL(
        "Failed to create temporary file in '{}': {}",
        basePath,
        std::strerror(errno));
  }
  return {std::string(storage.data()), fd};
}

size_t writeToFile(char* ptr, size_t size, size_t nmemb, void* userdata) {
  const size_t total = size * nmemb;
  auto* file = static_cast<FILE*>(userdata);
  const size_t written = fwrite(ptr, size, nmemb, file);
  if (written != nmemb) {
    return 0;
  }
  return total;
}

struct AbortAfterFirstChunkContext {
  bool aborted{false};
};

size_t abortAfterFirstChunk(char*, size_t size, size_t nmemb, void* userdata) {
  if (size == 0 || nmemb == 0) {
    return 0;
  }
  auto* ctx = static_cast<AbortAfterFirstChunkContext*>(userdata);
  ctx->aborted = true;
  return 0;
}

HeadProbeResult probeWithHead(const std::string& url) {
  CurlEasyHandle handle;
  auto* curl = handle.get();
  applyCommonCurlOptions(curl);
  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
  curl_easy_setopt(curl, CURLOPT_HEADER, 0L);
  curl_easy_setopt(curl, CURLOPT_HTTPGET, 0L);
  curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0L);

  const auto result = curl_easy_perform(curl);
  if (result != CURLE_OK) {
    VLOG(1) << fmt::format(
        "HTTP HEAD for '{}' failed with {} ({})",
        url,
        static_cast<int>(result),
        curl_easy_strerror(result));
    return {.status=HeadProbeStatus::kInconclusive, .contentLength=std::nullopt};
  }

  long responseCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode);
  if (responseCode == 404 || responseCode == 410) {
    return {.status=HeadProbeStatus::kNotFound, .contentLength=std::nullopt};
  }

  if (responseCode >= 200 && responseCode < 300) {
    curl_off_t contentLength = -1;
    auto infoRes = curl_easy_getinfo(
        curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &contentLength);
    if (infoRes == CURLE_OK && contentLength >= 0) {
      return {
          .status=HeadProbeStatus::kSuccess,
          .contentLength=static_cast<int64_t>(contentLength)};
    }
    return {.status=HeadProbeStatus::kSuccess, .contentLength=std::nullopt};
  }

  VLOG(1) << fmt::format(
      "HTTP HEAD for '{}' returned status {}",
      url,
      responseCode);
  return {.status=HeadProbeStatus::kInconclusive, .contentLength=std::nullopt};
}

struct CurlGlobalState {
  CurlGlobalState() = default;

  bool initialize() {
    std::scoped_lock lock(mutex_);
    if (initialized_) {
      return false;
    }

    auto code = curl_global_init(CURL_GLOBAL_DEFAULT);
    VELOX_CHECK_EQ(
        static_cast<int>(code),
        static_cast<int>(CURLE_OK),
        "curl_global_init failed with code {} ({})",
        static_cast<int>(code),
        curl_easy_strerror(code));
    initialized_ = true;
    return true;
  }

  void finalize() {
    std::scoped_lock lock(mutex_);
    if (!initialized_) {
      return;
    }
    curl_global_cleanup();
    initialized_ = false;
  }

  bool isInitialized() const {
    return initialized_.load();
  }

 private:
  std::atomic<bool> initialized_{false};
  std::mutex mutex_;
};

CurlGlobalState& curlState() {
  static CurlGlobalState state;
  return state;
}

class HttpReadFile : public ReadFile {
 public:
  explicit HttpReadFile(std::string url) : url_(std::move(url)) {}

  ~HttpReadFile() override {
    if (localReadFile_) {
      localReadFile_.reset();
    }
    if (!localPath_.empty()) {
      ::unlink(localPath_.c_str());
      localPath_.clear();
    }
  }

  void initialize(const FileOptions& options) {
    if (options.fileSize.has_value()) {
      const auto value = options.fileSize.value();
      VELOX_CHECK_GE(value, 0, "File size must not be negative");
      expectedSize_ = value;
      size_.store(value, std::memory_order_release);
      sizeKnown_.store(true, std::memory_order_release);
    }
  }

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buffer,
      const FileIoContext& context = {}) const override {
    VELOX_CHECK_GT(length, 0, "HTTP pread requires positive length");
    ensureDownloaded();
    auto data = localReadFile_->pread(offset, length, buffer, context);
    bytesRead_ += length;
    return data;
  }

  std::string pread(
      uint64_t offset,
      uint64_t length,
      const FileIoContext& context = {}) const override {
    if (length == 0) {
      return {};
    }
    std::string buffer(length, '\0');
    auto view = pread(offset, length, buffer.data(), context);
    buffer.resize(view.size());
    return buffer;
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      const FileIoContext& context = {}) const override {
    ensureDownloaded();
    auto bytes = localReadFile_->preadv(offset, buffers, context);
    bytesRead_ += bytes;
    return bytes;
  }

  uint64_t size() const override {
    ensureSize();
    auto value = size_.load(std::memory_order_acquire);
    VELOX_CHECK_GE(
        value, 0, "HTTP resource '{}' size is not available", url_);
    return static_cast<uint64_t>(value);
  }

  uint64_t memoryUsage() const override {
    uint64_t usage = localPath_.capacity();
    if (localReadFile_) {
      usage += localReadFile_->memoryUsage();
    }
    return usage;
  }

  bool shouldCoalesce() const override {
    return true;
  }

  std::string getName() const override {
    return url_;
  }

  uint64_t getNaturalReadSize() const override {
    return 8ULL << 20U; // Preserve previous 8MB hint for callers.
  }

 private:
  void ensureSize() const {
    if (sizeKnown_.load(std::memory_order_acquire)) {
      return;
    }
    {
      std::scoped_lock const lock(sizeMutex_);
      if (sizeKnown_.load(std::memory_order_relaxed)) {
        return;
      }

      const auto [status, contentLength] = probeWithHead(url_);
      if (status == HeadProbeStatus::kNotFound) {
        VELOX_FILE_NOT_FOUND_ERROR("HTTP resource '{}' not found", url_);
      }
      if (status == HeadProbeStatus::kSuccess &&
          contentLength.has_value()) {
        size_.store(contentLength.value(), std::memory_order_release);
        sizeKnown_.store(true, std::memory_order_release);
        return;
      }
    }
    ensureDownloaded();
  }

  void ensureDownloaded() const {
    std::call_once(downloadOnce_, [&]() { downloadToLocalFile(); });
    VELOX_CHECK_NOT_NULL(
        localReadFile_.get(),
        "Failed to materialize HTTP resource '{}' locally",
        url_);
  }

  void downloadToLocalFile() const {
    std::optional<int64_t> headLength;
    if (!sizeKnown_.load(std::memory_order_acquire)) {
      const auto [status, contentLength] = probeWithHead(url_);
      if (status == HeadProbeStatus::kNotFound) {
        VELOX_FILE_NOT_FOUND_ERROR("HTTP resource '{}' not found", url_);
      }
      if (status == HeadProbeStatus::kSuccess &&
          contentLength.has_value()) {
        headLength = contentLength;
      }
    }

    auto [tempPath, tempFd] = createTemporaryFile();
    auto cleanup = folly::makeGuard([&]() { ::unlink(tempPath.c_str()); });

    FILE* file = fdopen(tempFd, "wb");
    if (file == nullptr) {
      const auto err = errno;
      ::close(tempFd);
      VELOX_FAIL(
          "Failed to open temporary file '{}' for download: {}",
          tempPath,
          std::strerror(err));
    }

    CurlEasyHandle handle;
    auto* curl = handle.get();
    applyCommonCurlOptions(curl);
    curl_easy_setopt(curl, CURLOPT_URL, url_.c_str());
    curl_easy_setopt(curl, CURLOPT_NOBODY, 0L);
    curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToFile);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, file);

    const auto result = curl_easy_perform(curl);
    if (const int closeRes = fclose(file); closeRes != 0) {
      const auto err = errno;
      VELOX_FAIL(
          "Failed to close temporary file '{}' for '{}': {}",
          tempPath,
          url_,
          std::strerror(err));
    }

    if (result != CURLE_OK) {
      VELOX_FAIL(
          "HTTP GET request for '{}' failed with code {} ({})",
          url_,
          static_cast<int>(result),
          curl_easy_strerror(result));
    }

    long responseCode = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode);
    if (responseCode == 404 || responseCode == 410) {
      VELOX_FILE_NOT_FOUND_ERROR("HTTP resource '{}' not found", url_);
    }
    VELOX_CHECK(
        responseCode >= 200 && responseCode < 300,
        "Unexpected HTTP status {} for GET {}",
        responseCode,
        url_);

    struct stat file_stats{};
    if (::stat(tempPath.c_str(), &file_stats) != 0) {
      const auto err = errno;
      VELOX_FAIL(
          "stat failed for temporary file '{}' ({}): {}",
          tempPath,
          url_,
          std::strerror(err));
    }
    const auto downloadedSize = static_cast<int64_t>(file_stats.st_size);

    if (headLength.has_value() &&
        headLength.value() != downloadedSize) {
      VELOX_FAIL(
          "Size mismatch for '{}': expected {} bytes from HEAD but downloaded {} bytes",
          url_,
          headLength.value(),
          downloadedSize);
    }

    if (const auto previousSize = size_.load(std::memory_order_acquire); previousSize >= 0 && previousSize != downloadedSize) {
      VELOX_FAIL(
          "Size mismatch for '{}': expected {} bytes but downloaded {} bytes",
          url_,
          previousSize,
          downloadedSize);
    }

    if (expectedSize_.has_value() &&
        expectedSize_.value() != downloadedSize) {
      VELOX_FAIL(
          "Size mismatch for '{}': expected {} bytes but downloaded {} bytes",
          url_,
          expectedSize_.value(),
          downloadedSize);
    }

    size_.store(downloadedSize, std::memory_order_release);
    sizeKnown_.store(true, std::memory_order_release);
    localReadFile_ = std::make_unique<LocalReadFile>(tempPath);

    cleanup.dismiss();
    localPath_ = std::move(tempPath);
  }

  std::string url_;
  mutable std::atomic<int64_t> size_{-1};
  mutable std::atomic<bool> sizeKnown_{false};
  std::optional<int64_t> expectedSize_;
  mutable std::once_flag downloadOnce_;
  mutable std::unique_ptr<LocalReadFile> localReadFile_;
  mutable std::string localPath_;
  mutable std::mutex sizeMutex_;
};

} // namespace

bool initializeHttp() {
  return curlState().initialize();
}

void finalizeHttp() {
  curlState().finalize();
}

HttpFileSystem::HttpFileSystem(
    std::shared_ptr<const config::ConfigBase> config)
    : FileSystem(config), http_config_(config) {
  if (!curlState().isInitialized()) {
    LOG(WARNING) << "HttpFileSystem constructed before curl initialized. "
                 << "Calling initializeHttp() automatically.";
    initializeHttp();
  }
}

std::string HttpFileSystem::name() const {
  return "http";
}

std::string_view HttpFileSystem::extractPath(std::string_view path) const {
  if (path.starts_with(kHttpScheme)) {
    return path.substr(kHttpScheme.size());
  }
  if (path.starts_with(kHttpsScheme)) {
    return path.substr(kHttpsScheme.size());
  }
  return path;
}

std::unique_ptr<ReadFile> HttpFileSystem::openFileForRead(
    std::string_view path,
    const FileOptions& options) {
  auto file = std::make_unique<HttpReadFile>(std::string(path));
  file->initialize(options);
  return file;
}

std::unique_ptr<WriteFile> HttpFileSystem::openFileForWrite(
    std::string_view path,
    const FileOptions& /*options*/) {
  VELOX_UNSUPPORTED("HttpFileSystem is read-only. Cannot write '{}'.", path);
}

void HttpFileSystem::remove(std::string_view path) {
  VELOX_UNSUPPORTED("HttpFileSystem is read-only. Cannot remove '{}'.", path);
}

void HttpFileSystem::rename(
    std::string_view source,
    std::string_view destination,
    bool /*overwrite*/) {
  VELOX_UNSUPPORTED(
      "HttpFileSystem is read-only. Cannot rename '{}' to '{}'.",
      source,
      destination);
}

bool HttpFileSystem::exists(std::string_view path) {
  std::string url(path);
  const auto headResult = probeWithHead(url);
  if (headResult.status == HeadProbeStatus::kNotFound) {
    return false;
  }
  if (headResult.status == HeadProbeStatus::kSuccess) {
    return true;
  }

  CurlEasyHandle handle;
  auto* curl = handle.get();

  applyCommonCurlOptions(curl);
  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_NOBODY, 0L);
  curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
  curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0L);
  AbortAfterFirstChunkContext ctx;
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, abortAfterFirstChunk);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ctx);

  const auto result = curl_easy_perform(curl);
  if (result != CURLE_OK && result != CURLE_WRITE_ERROR) {
    LOG(WARNING) << fmt::format(
        "HTTP GET exists probe for '{}' failed with {} ({})",
        url,
        static_cast<int>(result),
        curl_easy_strerror(result));
    return false;
  }

  long responseCode = 0;
  curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode);
  if (responseCode == 404 || responseCode == 410) {
    return false;
  }
  if (responseCode >= 200 && responseCode < 300) {
    return true;
  }

  VELOX_FAIL(
      "Unexpected HTTP status {} while checking existence of '{}'",
      responseCode,
      url);
}

std::vector<std::string> HttpFileSystem::list(std::string_view path) {
  VELOX_UNSUPPORTED("HttpFileSystem does not support listing '{}'.", path);
}

void HttpFileSystem::mkdir(
    std::string_view path,
    const DirectoryOptions& /*options*/) {
  VELOX_UNSUPPORTED("HttpFileSystem is read-only. Cannot mkdir '{}'.", path);
}

void HttpFileSystem::rmdir(std::string_view path) {
  VELOX_UNSUPPORTED("HttpFileSystem is read-only. Cannot rmdir '{}'.", path);
}

} // namespace facebook::velox::filesystems
