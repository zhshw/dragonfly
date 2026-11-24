// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/transport/folly_rpc.h"

#include <folly/Singleton.h>
#include <folly/io/async/AsyncIoUringSocket.h>
#include <folly/io/async/AsyncServerSocket.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/IoUringBackend.h>
#include <folly/io/async/ScopedEventBaseThread.h>

#include <array>
#include <map>

namespace dfly::cluster::raft {
namespace {

using namespace std;

folly::EventBase::Options Options() {
  // Dragonfly initializes flags and logging itself; complete Folly singleton registration
  // without invoking folly::init, which would take over those process-wide responsibilities.
  folly::SingletonVault::singleton()->registrationComplete();
  return folly::EventBase::Options().setBackendFactory([] {
    folly::IoUringBackend::Options options;
    options.setCapacity(256).setMaxSubmit(128).setInitialProvidedBuffers(16U << 10, 128);
    return make_unique<folly::IoUringBackend>(std::move(options));
  });
}

error_code Error(const folly::AsyncSocketException& error) {
  if (error.getType() == folly::AsyncSocketException::TIMED_OUT)
    return make_error_code(errc::timed_out);
  return error.getErrno() ? error_code(error.getErrno(), generic_category())
                          : make_error_code(errc::connection_reset);
}

size_t FrameSize(string_view bytes, size_t max_frame) {
  if (bytes.size() < 4)
    return 0;
  uint32_t length = 0;
  for (unsigned char c : bytes.substr(0, 4))
    length = (length << 8) | c;
  return length > INT32_MAX || length > max_frame - min(max_frame, size_t{4}) ? SIZE_MAX
                                                                              : size_t{length} + 4;
}

}  // namespace

class FollyRpcClient::Impl : public folly::AsyncSocket::ConnectCallback,
                             public folly::AsyncTransport::ReadCallback,
                             public folly::AsyncTransport::WriteCallback,
                             public folly::AsyncTimeout {
 public:
  Impl() : thread_(Options(), nullptr, "raft-peer-uring") {
    thread_.getEventBase()->runInEventBaseThreadAndWait(
        [this] { attachEventBase(thread_.getEventBase()); });
  }

  ~Impl() {
    thread_.getEventBase()->runInEventBaseThreadAndWait([this] {
      Finish(make_error_code(errc::operation_canceled));
      socket_.reset();
      detachEventBase();
    });
  }

  void Exchange(string host, uint16_t port, string request, size_t max_frame, uint32_t timeout_ms,
                Reply reply) {
    thread_.getEventBase()->runInEventBaseThread([this, host = std::move(host), port,
                                                  request = std::move(request), max_frame,
                                                  timeout_ms, reply = std::move(reply)]() mutable {
      if (reply_) {
        reply(make_error_code(errc::operation_in_progress), {}, {}, 0);
        return;
      }
      reply_ = std::move(reply);
      request_ = std::move(request);
      max_frame_ = max_frame;
      received_ = 0;
      frame_size_ = 0;
      large_.reset();
      error_.clear();
      response_ready_ = false;
      scheduleTimeout(timeout_ms);
      if (socket_) {
        Send();
        return;
      }
      try {
        socket_.reset(new folly::AsyncIoUringSocket(thread_.getEventBase()));
        socket_->connect(this, folly::SocketAddress(host, port), chrono::milliseconds(timeout_ms));
      } catch (const folly::AsyncSocketException& e) {
        Finish(Error(e));
      } catch (const exception&) {
        Finish(make_error_code(errc::invalid_argument));
      }
    });
  }

  void Close(function<void()> done) {
    thread_.getEventBase()->runInEventBaseThread([this, done = std::move(done)] {
      Finish(make_error_code(errc::operation_canceled));
      socket_.reset();
      done();
    });
  }

 private:
  void connectSuccess() noexcept override {
    Send();
  }
  void connectErr(const folly::AsyncSocketException& e) noexcept override {
    Finish(Error(e));
  }
  void Send() {
    socket_->setReadCB(this);
    writing_ = true;
    socket_->write(this, request_.data(), request_.size());
  }
  void writeSuccess() noexcept override {
    writing_ = false;
    request_.clear();
    if (response_ready_ || error_)
      Deliver();
  }
  void writeErr(size_t, const folly::AsyncSocketException& e) noexcept override {
    writing_ = false;
    Finish(Error(e));
  }
  void getReadBuffer(void** bytes, size_t* size) override {
    *bytes = Data() + received_;
    *size = (large_ ? frame_size_ + 1 : first_.size()) - received_;
  }
  void readDataAvailable(size_t count) noexcept override {
    received_ += count;
    if (!frame_size_ && received_ >= 4) {
      frame_size_ = FrameSize({Data(), received_}, max_frame_);
      if (frame_size_ == SIZE_MAX || received_ > frame_size_) {
        Finish(make_error_code(errc::protocol_error));
        return;
      }
      if (frame_size_ >= first_.size() && received_ < frame_size_) {
        large_ = make_shared_for_overwrite<char[]>(frame_size_ + 1);
        copy_n(first_.data(), received_, large_.get());
      }
    }
    if (frame_size_ && received_ >= frame_size_) {
      if (received_ != frame_size_)
        Finish(make_error_code(errc::protocol_error));
      else {
        response_ready_ = true;
        socket_->setReadCB(nullptr);
        if (!writing_)
          Deliver();
      }
    }
  }
  void readEOF() noexcept override {
    Finish(make_error_code(errc::connection_reset));
  }
  void readErr(const folly::AsyncSocketException& e) noexcept override {
    Finish(Error(e));
  }
  void timeoutExpired() noexcept override {
    Finish(make_error_code(errc::timed_out));
  }
  char* Data() {
    return large_ ? large_.get() : first_.data();
  }
  void Finish(error_code error) {
    if (!reply_)
      return;
    error_ = error;
    cancelTimeout();
    if (socket_) {
      socket_->setReadCB(nullptr);

      // closeNow synchronously completes pending write callbacks before buffers can be released.
      socket_->closeNow();
    }
    if (!writing_)
      Deliver();
  }
  void Deliver() {
    if (!reply_)
      return;
    cancelTimeout();
    auto reply = std::move(reply_);
    shared_ptr<const void> owner;
    if (large_)
      owner = shared_ptr<const void>(large_, large_.get());
    reply(error_, error_ ? string_view{} : string_view(Data(), received_), owner,
          large_ ? frame_size_ + 1 : 0);
    large_.reset();
  }

  folly::ScopedEventBaseThread thread_;
  folly::AsyncIoUringSocket::UniquePtr socket_;
  Reply reply_;
  string request_;
  array<char, 4096> first_;
  shared_ptr<char[]> large_;
  size_t max_frame_ = 0, received_ = 0, frame_size_ = 0;
  bool writing_ = false, response_ready_ = false;
  error_code error_;
};

class FollyRpcServer::Impl : public folly::AsyncServerSocket::AcceptCallback {
  struct Connection : folly::AsyncTransport::ReadCallback,
                      folly::AsyncTransport::WriteCallback,
                      enable_shared_from_this<Connection> {
    Connection(Impl* server, uint64_t id, folly::NetworkSocket socket)
        : server(server), id(id), socket(new folly::AsyncIoUringSocket(server->Loop(), socket)) {
    }
    void getReadBuffer(void** bytes, size_t* size) override {
      *bytes = buffer.data();
      *size = buffer.size();
    }
    void readDataAvailable(size_t count) noexcept override {
      pending.append(buffer.data(), count);
      Next();
    }
    void Next() {
      const auto size = FrameSize(pending, server->max_frame_);
      if (size == SIZE_MAX) {
        Close();
        return;
      }
      if (!size || pending.size() < size) {
        socket->setReadCB(this);
        return;
      }
      socket->setReadCB(nullptr);
      auto request = make_shared<const string>(pending.substr(0, size));
      pending.erase(0, size);
      weak_ptr<Connection> weak = shared_from_this();
      auto* loop = server->Loop();
      if (!server->handler_(std::move(request), [weak, loop](error_code ec, string bytes) {
            loop->runInEventBaseThread([weak, ec, bytes = std::move(bytes)]() mutable {
              if (auto conn = weak.lock(); conn && conn->socket) {
                if (ec) {
                  conn->Close();
                  return;
                }
                conn->response = std::move(bytes);
                conn->socket->write(conn.get(), conn->response.data(), conn->response.size());
              }
            });
          }))
        Close();
    }
    void writeSuccess() noexcept override {
      response.clear();
      Next();
    }
    void writeErr(size_t, const folly::AsyncSocketException&) noexcept override {
      Close();
    }
    void readEOF() noexcept override {
      Close();
    }
    void readErr(const folly::AsyncSocketException&) noexcept override {
      Close();
    }
    void Close() {
      auto self = shared_from_this();
      if (!socket)
        return;
      auto closing = std::move(socket);
      closing->setReadCB(nullptr);
      closing->closeNow();
      server->connections_.erase(id);
    }
    Impl* server;
    uint64_t id;
    folly::AsyncIoUringSocket::UniquePtr socket;
    array<char, 4096> buffer;
    string pending, response;
  };

 public:
  Impl() : thread_(Options(), nullptr, "raft-listen-uring") {
  }
  ~Impl() {
    Stop();
  }
  folly::EventBase* Loop() {
    return thread_.getEventBase();
  }
  error_code Listen(const string& host, uint16_t port, size_t max_frame, Handler handler) {
    error_code error;
    Loop()->runInEventBaseThreadAndWait([&] {
      try {
        max_frame_ = max_frame;
        handler_ = std::move(handler);
        listener_ = folly::AsyncServerSocket::newSocket(Loop());
        listener_->bind(folly::SocketAddress(host, port));
        listener_->listen(128);
        folly::SocketAddress address;
        listener_->getAddress(&address);
        port_ = address.getPort();
        listener_->addAcceptCallback(this, Loop());
        listener_->startAccepting();
      } catch (const folly::AsyncSocketException& e) {
        error = Error(e);
      } catch (const exception&) {
        error = make_error_code(errc::invalid_argument);
      }
    });
    return error;
  }
  void Pause() {
    Loop()->runInEventBaseThreadAndWait([this] {
      if (listener_)
        listener_->pauseAccepting();
    });
  }
  void Stop() {
    Loop()->runInEventBaseThreadAndWait([this] {
      listener_.reset();
      while (!connections_.empty())
        connections_.begin()->second->Close();
    });
  }
  uint16_t port() const {
    return port_;
  }

 private:
  void connectionAccepted(folly::NetworkSocket fd, const folly::SocketAddress&,
                          folly::AsyncServerSocket::AcceptCallback::AcceptInfo) noexcept override {
    auto conn = make_shared<Connection>(this, ++next_id_, fd);
    connections_.emplace(next_id_, conn);
    if (connections_.size() > 128)
      conn->Close();
    else
      conn->socket->setReadCB(conn.get());
  }
  void acceptError(const exception&) noexcept override {
  }
  folly::ScopedEventBaseThread thread_;
  shared_ptr<folly::AsyncServerSocket> listener_;
  map<uint64_t, shared_ptr<Connection>> connections_;
  Handler handler_;
  uint64_t next_id_ = 0;
  size_t max_frame_ = 0;
  uint16_t port_ = 0;
};

FollyRpcClient::FollyRpcClient() : impl_(make_unique<Impl>()) {
}
FollyRpcClient::~FollyRpcClient() = default;
void FollyRpcClient::Exchange(string host, uint16_t port, string request, size_t max_frame,
                              uint32_t timeout_ms, Reply reply) {
  impl_->Exchange(std::move(host), port, std::move(request), max_frame, timeout_ms,
                  std::move(reply));
}
void FollyRpcClient::Close(function<void()> done) {
  impl_->Close(std::move(done));
}
FollyRpcServer::FollyRpcServer() : impl_(make_unique<Impl>()) {
}
FollyRpcServer::~FollyRpcServer() = default;
error_code FollyRpcServer::Listen(const string& host, uint16_t port, size_t max_frame,
                                  Handler handler) {
  return impl_->Listen(host, port, max_frame, std::move(handler));
}
uint16_t FollyRpcServer::port() const {
  return impl_->port();
}
void FollyRpcServer::Pause() {
  impl_->Pause();
}
void FollyRpcServer::Stop() {
  impl_->Stop();
}

}  // namespace dfly::cluster::raft
