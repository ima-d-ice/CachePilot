#include "admin.h"

#include "epoll_compat.h"
#include "storage.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace std;

namespace {

constexpr int kMaxEvents = 64;
constexpr size_t kMaxRequest = 4096;

string http_response(int status, const string& status_text,
                     const string& body) {
    string resp = "HTTP/1.1 ";
    resp += to_string(status);
    resp += ' ';
    resp += status_text;
    resp += "\r\n";
    resp += "Content-Type: application/json\r\n";
    resp += "Content-Length: ";
    resp += to_string(body.size());
    resp += "\r\n";
    resp += "Connection: close\r\n";
    resp += "\r\n";
    resp += body;
    return resp;
}

}  // namespace

AdminServer::AdminServer(int port, Storage* storage)
    : port_(port), storage_(storage) {}

AdminServer::~AdminServer() {
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

void AdminServer::start() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        perror("admin socket");
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
    if (::bind(listen_fd_, reinterpret_cast<const sockaddr*>(&addr),
               sizeof(addr)) < 0) {
        perror("admin bind");
        close(listen_fd_);
        listen_fd_ = -1;
        return;
    }
    if (listen(listen_fd_, SOMAXCONN) < 0) {
        perror("admin listen");
        close(listen_fd_);
        listen_fd_ = -1;
        return;
    }

    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        perror("admin epoll_create1");
        close(listen_fd_);
        listen_fd_ = -1;
        return;
    }

    if (socketpair(AF_UNIX, SOCK_STREAM, 0, wake_pipe_) < 0) {
        perror("admin socketpair");
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
                close_conn(fd);
                continue;
            }
            if ((event_flags & EPOLLIN) != 0) {
                handle_connection(fd);
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

void AdminServer::stop() {
    running_ = false;
    if (wake_pipe_[1] != -1) {
        const char byte = 1;
        (void)write(wake_pipe_[1], &byte, 1);
    }
}

void AdminServer::accept_connections() {
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
    }
}

void AdminServer::close_conn(int fd) {
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    close(fd);
}

void AdminServer::handle_connection(int fd) {
    string request;
    char buf[1024];
    for (;;) {
        const ssize_t n = recv(fd, buf, sizeof(buf), 0);
        if (n > 0) {
            request.append(buf, static_cast<size_t>(n));
            if (request.size() > kMaxRequest) {
                send_all(fd, http_response(400, "Bad Request", "{}"));
                close_conn(fd);
                return;
            }
            if (request.find("\r\n\r\n") != string::npos ||
                request.find("\n\n") != string::npos) {
                break;
            }
            continue;
        }
        if (n == 0) {
            if (request.empty()) {
                close_conn(fd);
                return;
            }
            break;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        close_conn(fd);
        return;
    }

    send_all(fd, handle_request(request));
    close_conn(fd);
}

string AdminServer::handle_request(const string& request) {
    const size_t line_end = request.find("\r\n");
    const size_t first_line_len =
        line_end == string::npos ? request.size() : line_end;
    const string first_line = request.substr(0, first_line_len);

    const size_t p1 = first_line.find(' ');
    const size_t p2 = p1 == string::npos ? string::npos
                                         : first_line.find(' ', p1 + 1);
    if (p1 == string::npos || p2 == string::npos) {
        return http_response(400, "Bad Request", "{}");
    }
    const string method = first_line.substr(0, p1);
    const string path = first_line.substr(p1 + 1, p2 - p1 - 1);

    if (method != "GET") {
        return http_response(405, "Method Not Allowed", "{}");
    }
    if (path == "/metrics") {
        return http_response(200, "OK", storage_->metrics().dump());
    }
    if (path == "/health") {
        return http_response(200, "OK", "{\"status\":\"ok\"}");
    }
    return http_response(404, "Not Found", "{}");
}

void AdminServer::send_all(int fd, const string& response) {
    size_t offset = 0;
    while (offset < response.size()) {
        const ssize_t n = send(fd, response.data() + offset,
                               response.size() - offset,
                               COMPAT_MSG_NOSIGNAL);
        if (n > 0) {
            offset += static_cast<size_t>(n);
            continue;
        }
        if (n < 0 && errno == EAGAIN) {
            pollfd pfd{fd, POLLOUT, 0};
            (void)poll(&pfd, 1, 100);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        return;
    }
}