#include "velox/connectors/hive/storage_adapters/http/HttpFileSystem.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"

#include <curl/curl.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <folly/Range.h>

#include <atomic>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <cstring>
#include <memory>

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

struct WriteCtx {
  char* data;
  size_t capacity;
  size_t offset{0};
  bool overflow{false};
} __attribute__((aligned(32)));

size_t writeToFixedBuffer(char* ptr, size_t size, size_t nmemb, void* userdata) {
  auto* ctx = static_cast<WriteCtx*>(userdata);
  const size_t bytes = size * nmemb;
  if (ctx->offset + bytes > ctx->capacity) {
    ctx->overflow = true;
    return 0;
  }
  memcpy(ctx->data + ctx->offset, ptr, bytes);
  ctx->offset += bytes;
  return bytes;
}

} // namespace

namespace {

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
  HttpReadFile(
      std::string url)
      : url_(std::move(url)) {}

  void initialize(const FileOptions& options) {
    if (options.fileSize.has_value()) {
      const auto value = options.fileSize.value();
      VELOX_CHECK_GE(value, 0, "File size must not be negative");
      size_.store(value, std::memory_order_release);
      folly::set_once(sizeInitialized_);
    }
  }

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buffer,
      File::IoStats* /*stats*/) const override {
    VELOX_CHECK_GT(length, 0, "HTTP pread requires positive length");
    performRead(offset, length, static_cast<char*>(buffer));
    bytesRead_ += length;
    return {static_cast<char*>(buffer), length};
  }

  std::string pread(
      uint64_t offset,
      uint64_t length,
      File::IoStats* /*stats*/) const override {
    std::string data(length, '\0');
    performRead(offset, length, data.data());
    bytesRead_ += length;
    return data;
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers,
      File::IoStats* /*stats*/) const override {
    size_t total = 0;
    for (const auto& range : buffers) {
      total += range.size();
    }

    if (total == 0) {
      return 0;
    }

    std::string temp(total, '\0');
    performRead(offset, total, temp.data());

    size_t cursor = 0;
    for (const auto& range : buffers) {
      if (range.data() != nullptr && !range.empty()) {
        memcpy(range.data(), temp.data() + cursor, range.size());
      }
      cursor += range.size();
    }
    bytesRead_ += total;
    return total;
  }

  uint64_t size() const override {
    ensureSize();
    return static_cast<uint64_t>(size_.load(std::memory_order_relaxed));
  }

  uint64_t memoryUsage() const override {
    return sizeof(HttpReadFile);
  }

  bool shouldCoalesce() const override {
    return true;
  }

  std::string getName() const override {
    return url_;
  }

  uint64_t getNaturalReadSize() const override {
    return 8 << 20; // 8MB fetches strike a balance for HTTP downloads.
  }

 private:
  void ensureSize() const {
    folly::call_once(sizeInitialized_, [&]() {
      size_.store(fetchContentLength(), std::memory_order_release);
    });
  }

  int64_t fetchContentLength() const {
    CurlEasyHandle handle;
    auto* curl = handle.get();
    applyCommonCurlOptions(curl);
    curl_easy_setopt(curl, CURLOPT_URL, url_.c_str());
    curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
    curl_easy_setopt(curl, CURLOPT_HEADER, 0L);
    curl_easy_setopt(curl, CURLOPT_HTTPGET, 0L);
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0L);

    const auto result = curl_easy_perform(curl);
    VELOX_CHECK_EQ(
        static_cast<int>(result),
        static_cast<int>(CURLE_OK),
        "HTTP HEAD request for '{}' failed with code {} ({})",
        url_,
        static_cast<int>(result),
        curl_easy_strerror(result));

    long responseCode = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &responseCode);
    if (responseCode == 404 || responseCode == 410) {
      VELOX_FILE_NOT_FOUND_ERROR("HTTP resource '{}' not found", url_);
    }
    VELOX_CHECK(
        responseCode >= 200 && responseCode < 300,
        "Unexpected HTTP status {} for HEAD {}",
        responseCode,
        url_);

    curl_off_t contentLength = -1;
    auto infoRes = curl_easy_getinfo(
        curl, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &contentLength);
    VELOX_CHECK_EQ(
        static_cast<int>(infoRes),
        static_cast<int>(CURLE_OK),
        "Failed to obtain Content-Length for '{}'", url_);
    VELOX_CHECK_GT(
        contentLength,
        -1,
        "Server did not provide Content-Length for '{}'", url_);

    return static_cast<int64_t>(contentLength);
  }

  void performRead(uint64_t offset, uint64_t length, char* output) const {
    const auto maxSize = static_cast<uint64_t>(std::numeric_limits<size_t>::max());
    VELOX_CHECK_LE(
        length,
        maxSize,
        "Requested HTTP read size {} exceeds size_t limit {}",
        length,
        maxSize);
    CurlEasyHandle handle;
    auto* curl = handle.get();
    applyCommonCurlOptions(curl);
    curl_easy_setopt(curl, CURLOPT_URL, url_.c_str());
    curl_easy_setopt(curl, CURLOPT_NOBODY, 0L);
    curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(curl, CURLOPT_FAILONERROR, 1L);

    auto range = fmt::format("{}-{}", offset, offset + length - 1);
    curl_easy_setopt(curl, CURLOPT_RANGE, range.c_str());

    WriteCtx ctx{.data=output, .capacity=static_cast<size_t>(length)};
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, writeToFixedBuffer);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &ctx);

    const auto result = curl_easy_perform(curl);
    VELOX_CHECK_EQ(
        static_cast<int>(result),
        static_cast<int>(CURLE_OK),
        "HTTP GET request for '{}' failed with code {} ({})",
        url_,
        static_cast<int>(result),
        curl_easy_strerror(result));
    VELOX_CHECK( 
        !ctx.overflow,
        "Received more data than expected while reading '{}'",
        url_);
    VELOX_CHECK_EQ(
        ctx.offset,
        length,
        "Short HTTP read for '{}' expected {} got {} bytes",
        url_,
        length,
        ctx.offset);
  }

  std::string url_;
  mutable std::atomic<int64_t> size_{-1};
  mutable folly::once_flag sizeInitialized_;
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
    : FileSystem(std::move(config)) {
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
  CurlEasyHandle handle;
  auto* curl = handle.get();
  applyCommonCurlOptions(curl);
  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
  curl_easy_setopt(curl, CURLOPT_NOBODY, 1L);
  curl_easy_setopt(curl, CURLOPT_FAILONERROR, 0L);

  const auto result = curl_easy_perform(curl);
  if (result != CURLE_OK) {
    LOG(WARNING) << fmt::format(
        "HTTP HEAD for '{}' failed with {} ({})",
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
