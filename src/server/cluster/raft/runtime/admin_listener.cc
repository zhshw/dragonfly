// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/cluster/raft/runtime/admin_listener.h"

#include <absl/strings/match.h>

#include <algorithm>
#include <array>

#include "base/flags.h"
#include "facade/reply_builder.h"
#include "facade/resp_srv_parser.h"
#include "util/accept_server.h"
#include "util/connection.h"
#include "util/fibers/pool.h"
#include "util/listener_interface.h"

#ifdef DFLY_USE_SSL
#include "facade/tls_helpers.h"
#include "util/tls/tls_socket.h"
#endif

ABSL_DECLARE_FLAG(bool, tls);
ABSL_DECLARE_FLAG(bool, no_tls_on_admin_port);

namespace dfly::cluster::raft {
namespace {

using namespace std;
using absl::EqualsIgnoreCase;
using facade::CollectionType;
using facade::RedisReplyBuilder;
using Args = span<const string_view>;

void SendReply(const AdminReply& reply, RedisReplyBuilder* rb) {
  using Kind = AdminReply::Kind;
  switch (reply.kind) {
    case Kind::kSimple:
      return rb->SendSimpleString(reply.text);
    case Kind::kError: {
      string error = "-" + reply.text;
      replace(error.begin(), error.end(), '\r', ' ');
      replace(error.begin(), error.end(), '\n', ' ');
      return rb->SendError(error, "controller_admin");
    }
    case Kind::kNull:
      return rb->SendNull();
    case Kind::kBulk:
      return rb->SendBulkString(reply.text);
    case Kind::kMap:
    case Kind::kArray: {
      RedisReplyBuilder::ReplyScope scope(rb);
      const bool map = reply.kind == Kind::kMap;
      rb->StartCollection(map ? reply.elements.size() / 2 : reply.elements.size(),
                          map ? CollectionType::MAP : CollectionType::ARRAY);
      for (const auto& element : reply.elements) {
        if (element)
          rb->SendBulkString(*element);
        else
          rb->SendNull();
      }
      return;
    }
  }
}

class AdminConnection final : public util::Connection {
 public:
  AdminConnection(const string& password, const AdminHandler& handler)
      : password_(password), handler_(handler), authenticated_(password.empty()) {
  }

#ifdef DFLY_USE_SSL
  SSL_CTX* tls_context = nullptr;
#endif

 private:
  bool Authenticate(string_view user, string_view password, RedisReplyBuilder* rb) {
    if (password_.empty()) {
      rb->SendError("AUTH called without a password configured for the admin port");
      return false;
    }
    authenticated_ = (user == "default" && password == password_);
    if (!authenticated_)
      rb->SendError("-WRONGPASS invalid username-password pair");
    return authenticated_;
  }

  void Hello(Args args, RedisReplyBuilder* rb) {
    auto version = rb->GetRespVersion();
    if (args.size() >= 2) {
      if (args[1] != "2" && args[1] != "3")
        return rb->SendError("-NOPROTO unsupported protocol version");
      version = args[1] == "3" ? facade::RespVersion::kResp3 : facade::RespVersion::kResp2;
    }
    optional<pair<string_view, string_view>> credentials;
    optional<string_view> client_name;
    for (size_t i = 2; i < args.size();) {
      if (EqualsIgnoreCase(args[i], "AUTH") && i + 2 < args.size()) {
        credentials.emplace(args[i + 1], args[i + 2]);
        i += 3;
      } else if (EqualsIgnoreCase(args[i], "SETNAME") && i + 1 < args.size()) {
        client_name = args[i + 1];
        i += 2;
      } else {
        return rb->SendError("syntax error");
      }
    }
    if (credentials && !Authenticate(credentials->first, credentials->second, rb))
      return;
    if (!authenticated_)
      return rb->SendError("-NOAUTH Authentication required.");
    if (client_name)
      client_name_ = *client_name;
    rb->SetRespVersion(version);
    RedisReplyBuilder::ReplyScope scope(rb);
    rb->StartCollection(3, CollectionType::MAP);
    rb->SendBulkString("server");
    rb->SendBulkString("dragonfly");
    rb->SendBulkString("proto");
    rb->SendLong(rb->IsResp3() ? 3 : 2);
    rb->SendBulkString("mode");
    rb->SendBulkString("standalone");
  }

  void Dispatch(Args args, RedisReplyBuilder* rb) {
    const auto name = args.front();
    if (EqualsIgnoreCase(name, "HELLO"))
      return Hello(args, rb);
    if (EqualsIgnoreCase(name, "AUTH")) {
      if (args.size() != 2 && args.size() != 3)
        return rb->SendError("wrong number of arguments for 'auth' command");
      if (Authenticate(args.size() == 2 ? "default" : args[1], args.back(), rb))
        rb->SendOk();
      return;
    }
    if (!authenticated_)
      return rb->SendError("-NOAUTH Authentication required.");
    if (EqualsIgnoreCase(name, "PING") && args.size() == 1)
      return rb->SendSimpleString("PONG");
    if ((EqualsIgnoreCase(name, "PING") || EqualsIgnoreCase(name, "ECHO")) && args.size() == 2)
      return rb->SendBulkString(args[1]);
    if (EqualsIgnoreCase(name, "SELECT") && args.size() == 2 && args[1] == "0")
      return rb->SendOk();
    if (EqualsIgnoreCase(name, "CLIENT") && args.size() >= 2) {
      if (EqualsIgnoreCase(args[1], "SETINFO") && args.size() == 4 &&
          (EqualsIgnoreCase(args[2], "LIB-NAME") || EqualsIgnoreCase(args[2], "LIB-VER")))
        return rb->SendOk();
      if (EqualsIgnoreCase(args[1], "SETNAME") && args.size() == 3) {
        client_name_ = args[2];
        return rb->SendOk();
      }
      if (EqualsIgnoreCase(args[1], "GETNAME") && args.size() == 2)
        return client_name_.empty() ? rb->SendNull() : rb->SendBulkString(client_name_);
    }
    SendReply(handler_(args), rb);
  }

  void HandleRequests() final {
#ifdef DFLY_USE_SSL
    if (tls_context) {
      auto tls = make_unique<util::tls::TlsSocket>(std::move(socket_));
      tls->InitSSL(tls_context);
      SetSocket(tls.release());
      if (!socket_->Accept())
        return;
    }
#endif
    RedisReplyBuilder rb(socket());
    facade::RespSrvParser parser(64, 64 * 1024);
    cmn::BackedArguments command;
    vector<string_view> args;
    array<uint8_t, 4096> buffer;
    while (!rb.GetError()) {
      auto n = socket_->Recv(io::MutableBytes(buffer));
      if (!n || !*n)
        return;
      facade::RespSrvParser::Buffer pending(buffer.data(), *n);
      while (!pending.empty()) {
        uint32_t consumed = 0;
        auto result = parser.Parse(pending, &consumed, &command);
        pending.remove_prefix(consumed);
        if (result == facade::RespSrvParser::INPUT_PENDING)
          break;
        if (result != facade::RespSrvParser::OK) {
          rb.SendProtocolError("invalid admin command");
          return;
        }
        args.clear();
        for (auto arg : command.view())
          args.push_back(arg);
        if (args.size() == 1 && EqualsIgnoreCase(args.front(), "QUIT")) {
          rb.SendOk();
          return;
        }
        Dispatch(args, &rb);
        if (rb.GetError())
          return;
      }
    }
  }

  const string& password_;
  const AdminHandler& handler_;
  bool authenticated_;
  string client_name_;
};

class AdminListener final : public util::ListenerInterface {
 public:
  AdminListener(string password, AdminHandler handler)
      : password_(std::move(password)), handler_(std::move(handler)) {
  }

#ifdef DFLY_USE_SSL
  unique_ptr<SSL_CTX, decltype(&SSL_CTX_free)> tls_context{nullptr, SSL_CTX_free};
#endif

 private:
  util::Connection* NewConnection(util::fb2::ProactorBase*) final {
    auto* conn = new AdminConnection(password_, handler_);
#ifdef DFLY_USE_SSL
    conn->tls_context = tls_context.get();
#endif
    return conn;
  }

  string password_;
  AdminHandler handler_;
};

}  // namespace

absl::Status AddControllerAdminListener(const ControllerAdminConfig& config, AdminHandler handler,
                                        util::ProactorPool* pool, util::AcceptServer* acceptor) {
  if (!config.port)
    return absl::OkStatus();
  auto listener = make_unique<AdminListener>(config.password, std::move(handler));
  if (absl::GetFlag(FLAGS_tls) && !absl::GetFlag(FLAGS_no_tls_on_admin_port)) {
#ifdef DFLY_USE_SSL
    listener->tls_context.reset(facade::CreateSslCntx(facade::TlsContextRole::SERVER));
    if (!listener->tls_context)
      return absl::InvalidArgumentError("Cannot initialize controller admin TLS");
#else
    return absl::UnimplementedError("Controller admin TLS requires a TLS-enabled build");
#endif
  }

  // Controller-only processes do not construct the data server's thread-local facade stats.
  pool->Await([](auto*) {
    if (!facade::tl_facade_stats) {
      static thread_local facade::FacadeStats stats;
      facade::tl_facade_stats = &stats;
    }
  });
  const char* bind = config.bind_address.empty() ? nullptr : config.bind_address.c_str();
  if (auto ec = acceptor->AddListener(bind, config.port, listener.get()); ec)
    return absl::ErrnoToStatus(ec.value(), "Binding controller admin listener");
  listener.release();
  return absl::OkStatus();
}

}  // namespace dfly::cluster::raft
