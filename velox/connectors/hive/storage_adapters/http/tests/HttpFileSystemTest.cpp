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

#include "velox/connectors/hive/storage_adapters/http/HttpFileSystem.h"

#include "velox/common/base/Exceptions.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/File.h"

#include <gtest/gtest.h>

#include <fmt/format.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <array>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <future>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace facebook::velox::filesystems {
namespace {

class TestHttpServer {
 public:
  explicit TestHttpServer(
      std::unordered_map<std::string, std::string> files)
      : files_(std::move(files)) {
    serverThread_ = std::thread([this]() { run(); });
    port_ = portPromise_.get_future().get();
  }

  ~TestHttpServer() {
    stop();
  }

  uint16_t port() const {
    return port_;
  }

  void stop() {
    if (stopped_.exchange(true)) {
      return;
    }
    shouldStop_.store(true);
    if (port_ != 0) {
      int fd = ::socket(AF_INET, SOCK_STREAM, 0);
      if (fd >= 0) {
        sockaddr_in addr{
            .sin_family = AF_INET,
            .sin_port = htons(port_),
            .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)},
        };
        ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::close(fd);
      }
    }
    if (serverThread_.joinable()) {
      serverThread_.join();
    }
  }

 private:
  void run() {
    int const listenFd = ::socket(AF_INET, SOCK_STREAM, 0);
    VELOX_CHECK_GE(listenFd, 0, "Failed to create socket");

    int enable = 1;
    ::setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable));

    sockaddr_in addr{
        .sin_family = AF_INET,
        .sin_port = htons(0),
        .sin_addr = {.s_addr = htonl(INADDR_LOOPBACK)},
    };

    VELOX_CHECK_EQ(
        ::bind(listenFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)),
        0,
        "Failed to bind test HTTP server");
    VELOX_CHECK_EQ(::listen(listenFd, 8), 0, "listen failed");

    sockaddr_in local{};
    socklen_t len = sizeof(local);
    VELOX_CHECK_EQ(
        ::getsockname(listenFd, reinterpret_cast<sockaddr*>(&local), &len),
        0,
        "getsockname failed");
    portPromise_.set_value(ntohs(local.sin_port));

    while (!shouldStop_.load()) {
      sockaddr_in clientAddr{};
      socklen_t clientLen = sizeof(clientAddr);
      int const clientFd = ::accept(
          listenFd, reinterpret_cast<sockaddr*>(&clientAddr), &clientLen);
      if (clientFd < 0) {
        if (errno == EINTR) {
          continue;
        }
        if (shouldStop_.load()) {
          break;
        }
        continue;
      }
      handleClient(clientFd);
      ::close(clientFd);
    }

    ::close(listenFd);
  }

  void handleClient(int clientFd) {
    std::string request;
    request.reserve(1024);
    char buffer[1024];
    while (request.find("\r\n\r\n") == std::string::npos) {
      constexpr int noFlags = 0x0;
      ssize_t const bytes = ::recv(clientFd, buffer, sizeof(buffer), noFlags);
      if (bytes <= 0) {
        break;
      }
      request.append(buffer, buffer + bytes);
    }

    if (request.empty()) {
      return;
    }

    auto lineEnd = request.find("\r\n");
    if (lineEnd == std::string::npos) {
      return;
    }
    std::string firstLine = request.substr(0, lineEnd);
    const bool isHead = firstLine.starts_with("HEAD");
    const bool isGet = firstLine.starts_with("GET");
    if (!isHead && !isGet) {
      respond(clientFd, "405 Method Not Allowed", "");
      return;
    }

    const auto pathStart = firstLine.find(' ');
    const auto pathEnd = firstLine.find(' ', pathStart + 1);
    if (pathStart == std::string::npos || pathEnd == std::string::npos) {
      respond(clientFd, "400 Bad Request", "");
      return;
    }
    std::string path = firstLine.substr(pathStart + 1, pathEnd - pathStart - 1);

    auto it = files_.find(path);
    if (it == files_.end()) {
      respond(clientFd, "404 Not Found", "");
      return;
    }

    const std::string& content = it->second;
    if (isHead) {
      respond(
          clientFd,
          "200 OK",
          fmt::format(
              "Content-Length: {}\r\nAccept-Ranges: bytes\r\n", content.size()));
      return;
    }

    size_t offset = 0;
    size_t end = !content.empty() ? content.size() - 1 : 0;

    auto rangePos = request.find("Range:");
    if (rangePos != std::string::npos) {
      auto equalsPos = request.find('=', rangePos);
      auto dashPos = request.find('-', equalsPos);
      if (equalsPos != std::string::npos && dashPos != std::string::npos) {
        auto rangeLineEnd = request.find("\r\n", dashPos);
        std::string startStr = request.substr(equalsPos + 1, dashPos - equalsPos - 1);
        std::string endStr = request.substr(
            dashPos + 1,
            rangeLineEnd == std::string::npos ? std::string::npos
                                               : rangeLineEnd - dashPos - 1);
        offset = std::stoull(startStr);
        if (!endStr.empty()) {
          end = std::stoull(endStr);
        } else {
          end = content.size() - 1;
        }
      }
    }

    if (offset >= content.size()) {
      respond(clientFd, "416 Range Not Satisfiable", "");
      return;
    }

    end = std::min(end, content.size() - 1);
    const size_t length = end >= offset ? end - offset + 1 : 0;
    std::string headers = fmt::format(
        "Content-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\n"
        "Accept-Ranges: bytes\r\n",
        length,
        offset,
        offset + length - 1,
        content.size());
    respond(clientFd, "206 Partial Content", headers, content.substr(offset, length));
  }

  void respond(
      int clientFd,
      std::string_view status,
      std::string_view headers,
      std::string_view body = "") {
    std::string response = fmt::format(
        "HTTP/1.1 {}\r\n{}Connection: close\r\n\r\n",
        status,
        headers);
    if (!body.empty()) {
      response.append(body);
    }
    ::send(clientFd, response.data(), response.size(), 0);
  }

  std::unordered_map<std::string, std::string> files_;
  std::promise<uint16_t> portPromise_;
  std::thread serverThread_;
  std::atomic<bool> shouldStop_{false};
  std::atomic<bool> stopped_{false};
  uint16_t port_{0};
};

class HttpFileSystemTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    initializeHttp();
  }

  void SetUp() override {
    auto config = std::make_shared<const config::ConfigBase>(
        std::unordered_map<std::string, std::string>{});
    fs_ = std::make_unique<HttpFileSystem>(config);
  }

  std::unique_ptr<HttpFileSystem> fs_;
};

TEST_F(HttpFileSystemTest, BasicRead) {
  const std::string kPath = "/data";
  const std::string kContent = "abcdefghijklmnopqrstuvwxyz";
  TestHttpServer server({{kPath, kContent}});

  auto url = fmt::format("http://127.0.0.1:{}{}", server.port(), kPath);
  auto file = fs_->openFileForRead(url);

  std::array<char, 5> buffer{};
  auto view = file->pread(0, buffer.size(), buffer.data());
  EXPECT_EQ(std::string(view), kContent.substr(0, buffer.size()));

  auto tail = file->pread(13, 4);
  EXPECT_EQ(tail, kContent.substr(13, 4));

  EXPECT_EQ(file->size(), kContent.size());
}

TEST_F(HttpFileSystemTest, VectorRead) {
  const std::string kPath = "/vector";
  const std::string kContent = "0123456789abcdefghij";
  TestHttpServer server({{kPath, kContent}});

  auto url = fmt::format("http://127.0.0.1:{}{}", server.port(), kPath);
  auto file = fs_->openFileForRead(url);

  std::array<char, 4> first{};
  std::array<char, 6> second{};
  std::vector<folly::Range<char*>> ranges{
      {first.data(), first.size()},
      {nullptr, 3},
      {second.data(), second.size()}};

  auto bytes = file->preadv(2, ranges);
  EXPECT_EQ(bytes, first.size() + 3 + second.size());
  EXPECT_EQ(std::string(first.data(), first.size()), kContent.substr(2, 4));
  EXPECT_EQ(
      std::string(second.data(), second.size()),
      kContent.substr(2 + 4 + 3, second.size()));
}

TEST_F(HttpFileSystemTest, ExistsChecks) {
  TestHttpServer server({{"/exists", "payload"}});

  auto base = fmt::format("http://127.0.0.1:{}", server.port());
  EXPECT_TRUE(fs_->exists(base + "/exists"));
  EXPECT_FALSE(fs_->exists(base + "/missing"));
}

TEST_F(HttpFileSystemTest, MissingFileThrows) {
  TestHttpServer server({});
  auto url = fmt::format("http://127.0.0.1:{}/none", server.port());
  auto file = fs_->openFileForRead(url);
  try {
    file->size();
    FAIL() << "Expected size() to throw";
  } catch (const VeloxRuntimeError& e) {
    EXPECT_NE(e.message().find("HTTP resource"), std::string::npos)
        << "Unexpected message: " << e.message();
  }
}

} // namespace
} // namespace facebook::velox::filesystems
