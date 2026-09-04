#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>

#include "protocol.h"

class Storage;
class AOFLogger;

class Server {
public:
    Server(int port, Storage* storage, AOFLogger* aof);
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    void start();
    void stop();

private:
    static constexpr std::size_t kMaxBufferSize = std::size_t{64} * 1024;

    void accept_connections();
    void handle_readable(int fd);
    void process_frames(int fd);
    void queue_response(int fd, const std::string& response);
    void try_send(int fd);
    void update_watch(int fd);
    void close_client(int fd);
    std::string execute_command(const protocol::Command& cmd);

    int port_;
    Storage* storage_;
    AOFLogger* aof_;

    int listen_fd_ = -1;
    int epoll_fd_ = -1;
    int wake_pipe_[2] = {-1, -1};
    bool running_ = false;

    std::unordered_map<int, std::string> buffers_;
    std::unordered_map<int, std::string> outbox_;
    std::unordered_map<int, std::uint32_t> watch_flags_;
};
