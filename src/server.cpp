#include "server.h"

#include "aof.h"
#include "epoll_compat.h"
#include "protocol.h"
#include "storage.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace std;

namespace {

constexpr int kMaxEvents = 64;
constexpr int kReadChunk = 4096;

}  // namespace

Server::Server(int port, Storage* storage, AOFLogger* aof)
    : port_(port), storage_(storage), aof_(aof) {}

Server::~Server() {
    if (listen_fd_ != -1) {
        close(listen_fd_);
    }
    if (epoll_fd_ != -1) {
        close(epoll_fd_);
    }
    if (wake_pipe_[0] != -1) {
        close(wake_pipe_[0]);
    }
    if (wake_pipe_[1] != -1) {
        close(wake_pipe_[1]);
    }
}

void Server::start() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        perror("socket");
        return;
    }
    int flags = fcntl(listen_fd_, F_GETFL, 0);
    fcntl(listen_fd_, F_SETFL, flags | O_NONBLOCK);

    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port_));
    if (::bind(listen_fd_, reinterpret_cast<const sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind");
        close(listen_fd_);
        listen_fd_ = -1;
        return;
    }
    if (listen(listen_fd_, SOMAXCONN) < 0) {
        perror("listen");
        close(listen_fd_);
        listen_fd_ = -1;
        return;
    }

    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        perror("epoll_create1");
        close(listen_fd_);
        listen_fd_ = -1;
        return;
    }

    if (socketpair(AF_UNIX, SOCK_STREAM, 0, wake_pipe_) < 0) {
        perror("socketpair");
        close(epoll_fd_);
        epoll_fd_ = -1;
        close(listen_fd_);
        listen_fd_ = -1;
        return;
    }
    int wake_flags = fcntl(wake_pipe_[0], F_GETFL, 0);
    fcntl(wake_pipe_[0], F_SETFL, wake_flags | O_NONBLOCK);
    wake_flags = fcntl(wake_pipe_[1], F_GETFL, 0);
    fcntl(wake_pipe_[1], F_SETFL, wake_flags | O_NONBLOCK);

    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = listen_fd_;
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev);

    ev.data.fd = wake_pipe_[0];
    epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, wake_pipe_[0], &ev);

    running_ = true;
    epoll_event events[kMaxEvents];
    while (running_) {
        const int n = epoll_wait(epoll_fd_, events, kMaxEvents, -1);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        for (int i = 0; i < n; ++i) {
            const int fd = events[i].data.fd;
            const uint32_t event_flags = events[i].events;
            if (fd == wake_pipe_[0]) {
                char discard[64];
                while (recv(fd, discard, sizeof(discard), 0) > 0) {
                }
                continue;
            }
            if (fd == listen_fd_) {
                accept_connections();
                continue;
            }
            if ((event_flags & (EPOLLERR | EPOLLHUP)) != 0 &&
                (event_flags & EPOLLIN) == 0) {
                close_client(fd);
                continue;
            }
            if ((event_flags & EPOLLIN) != 0) {
                handle_readable(fd);
                if (buffers_.find(fd) == buffers_.end()) {
                    continue;
                }
            }
            if ((event_flags & EPOLLOUT) != 0) {
                try_send(fd);
            }
            if ((event_flags & (EPOLLERR | EPOLLHUP)) != 0 &&
                buffers_.find(fd) != buffers_.end()) {
                close_client(fd);
            }
        }
    }

    close(listen_fd_);
    listen_fd_ = -1;
    close(epoll_fd_);
    epoll_fd_ = -1;
    close(wake_pipe_[0]);
    close(wake_pipe_[1]);
    wake_pipe_[0] = -1;
    wake_pipe_[1] = -1;
}

void Server::stop() {
    running_ = false;
    if (wake_pipe_[1] != -1) {
        const char byte = 1;
        (void)write(wake_pipe_[1], &byte, 1);
    }
}

void Server::accept_connections() {
    for (;;) {
        const int client_fd = accept(listen_fd_, nullptr, nullptr);
        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }
            return;
        }
        int flags = fcntl(client_fd, F_GETFL, 0);
        fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);
        COMPAT_SET_NOSIGPIPE(client_fd);

        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = client_fd;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, client_fd, &ev);

        buffers_[client_fd].clear();
        watch_flags_[client_fd] = EPOLLIN;
    }
}

void Server::handle_readable(int fd) {
    char buf[kReadChunk];
    for (;;) {
        const ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n > 0) {
            string& buffer = buffers_[fd];
            buffer.append(buf, static_cast<size_t>(n));
            if (buffer.size() > kMaxBufferSize) {
                close_client(fd);
                return;
            }
            continue;
        }
        if (n == 0) {
            // Peer closed (or half-closed) the connection: process any
            // complete frames still buffered before tearing it down, so a
            // client that sends a command and then closes is still served.
            process_frames(fd);
            // process_frames may have already closed the client (e.g. on a
            // send error during response), so only close if still alive.
            if (buffers_.find(fd) != buffers_.end()) {
                close_client(fd);
            }
            return;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        close_client(fd);
        return;
    }
    process_frames(fd);
}

void Server::process_frames(int fd) {
    for (;;) {
        auto it = buffers_.find(fd);
        if (it == buffers_.end()) {
            return;
        }
        string& buffer = it->second;
        size_t consumed = 0;
        const auto cmd = protocol::try_parse(buffer, consumed);
        if (!cmd) {
            return;  // incomplete frame; wait for more bytes
        }
        buffer.erase(0, consumed);
        if (cmd->type == protocol::Command::UNKNOWN && cmd->args.empty()) {
            // Blank line / keepalive with no verb: ignore, as the legacy
            // line protocol did, rather than replying with an error.
            if (buffer.empty()) {
                return;
            }
            continue;
        }
        queue_response(fd, execute_command(*cmd));
        if (buffers_.find(fd) == buffers_.end()) {
            return;
        }
    }
}

void Server::queue_response(int fd, const string& response) {
    string out = response;
    if (out.size() < 2 || out.compare(out.size() - 2, 2, "\r\n") != 0) {
        out += "\r\n";
    }
    outbox_[fd] += out;
    try_send(fd);
}

void Server::try_send(int fd) {
    auto it = outbox_.find(fd);
    if (it == outbox_.end()) {
        return;
    }
    while (!it->second.empty()) {
        const ssize_t n =
            send(fd, it->second.data(), it->second.size(), COMPAT_MSG_NOSIGNAL);
        if (n > 0) {
            it->second.erase(0, static_cast<size_t>(n));
            continue;
        }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            break;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        close_client(fd);
        return;
    }
    update_watch(fd);
}

void Server::update_watch(int fd) {
    uint32_t flags = EPOLLIN;
    auto it = outbox_.find(fd);
    if (it != outbox_.end() && !it->second.empty()) {
        flags |= EPOLLOUT;
    }
    auto watch = watch_flags_.find(fd);
    if (watch != watch_flags_.end() && watch->second == flags) {
        return;
    }
    epoll_event ev{};
    ev.events = flags;
    ev.data.fd = fd;
    epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
    watch_flags_[fd] = flags;
}

void Server::close_client(int fd) {
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
    buffers_.erase(fd);
    outbox_.erase(fd);
    watch_flags_.erase(fd);
}

string Server::execute_command(const protocol::Command& cmd) {
    switch (cmd.type) {
        case protocol::Command::PING: {
            if (cmd.args.empty()) {
                return "+PONG";
            }
            return "$" + to_string(cmd.args[0].size()) + "\r\n" + cmd.args[0];
        }
        case protocol::Command::SELECT:
            // PolyCache is single-database; SELECT is accepted and ignored.
            return "+OK";
        case protocol::Command::SET: {
            if (cmd.args.size() < 2) {
                return "-ERR wrong number of arguments for SET";
            }
            int ttl = 0;
            bool saw_ex_px = false;
            // Parse optional RESP-style modifiers (EX/PX). Any other trailing
            // token (NX/XX/GET/KEEPTTL) is accepted but ignored for now.
            for (size_t k = 2; k < cmd.args.size(); ++k) {
                string opt = cmd.args[k];
                transform(opt.begin(), opt.end(), opt.begin(),
                          [](unsigned char c) {
                              return static_cast<char>(toupper(c));
                          });
                if (opt == "EX" || opt == "PX") {
                    saw_ex_px = true;
                    if (k + 1 >= cmd.args.size()) {
                        return "-ERR syntax error";
                    }
                    try {
                        const int v = stoi(cmd.args[k + 1]);
                        ttl = (opt == "PX") ? v / 1000 : v;
                    } catch (...) {
                        return "-ERR invalid expire value";
                    }
                    ++k;
                }
            }
            // Legacy positional TTL fallback: if no EX/PX was seen and the
            // third argument is numeric, treat it as seconds (backward compat
            // with the line protocol "SET key value ttl").
            if (!saw_ex_px && cmd.args.size() >= 3) {
                try {
                    ttl = stoi(cmd.args[2]);
                } catch (...) {  // NOLINT(bugprone-empty-catch): non-numeric -> ignore, ttl stays 0
                }
            }
            storage_->set(cmd.args[0], cmd.args[1], ttl);
            if (aof_ != nullptr) {
                aof_->log_set(cmd.args[0], cmd.args[1], ttl);
            }
            return "+OK";
        }
        case protocol::Command::GET: {
            if (cmd.args.empty()) {
                return "-ERR wrong number of arguments for GET";
            }
            const auto value = storage_->get(cmd.args[0]);
            if (!value) {
                return "$-1";
            }
            return "$" + to_string(value->size()) + "\r\n" + *value;
        }
        case protocol::Command::DEL: {
            if (cmd.args.empty()) {
                return "-ERR wrong number of arguments for DEL";
            }
            const bool removed = storage_->del(cmd.args[0]);
            if (removed && aof_ != nullptr) {
                aof_->log_del(cmd.args[0]);
            }
            return removed ? ":1" : ":0";
        }
        case protocol::Command::METRICS: {
            const string json = storage_->metrics().dump();
            return "$" + to_string(json.size()) + "\r\n" + json;
        }
        case protocol::Command::SWITCH_POLICY: {
            if (cmd.args.empty()) {
                return "-ERR wrong number of arguments for SWITCH_POLICY";
            }
            const bool ok = storage_->switch_policy(cmd.args[0]);
            return ok ? "+OK" : "-ERR unknown policy: " + cmd.args[0];
        }
        case protocol::Command::UNKNOWN:
        default:
            return "-ERR unknown command";
    }
}
