#pragma once

#ifdef __linux__

#include <sys/epoll.h>

#define COMPAT_MSG_NOSIGNAL MSG_NOSIGNAL
#define COMPAT_SET_NOSIGPIPE(fd) ((void)0)

#else

#include <errno.h>
#include <poll.h>
#include <stdint.h>

#include <atomic>
#include <mutex>
#include <unordered_map>
#include <vector>

#ifndef EPOLLIN
#define EPOLLIN 0x001
#endif
#ifndef EPOLLOUT
#define EPOLLOUT 0x004
#endif
#ifndef EPOLLERR
#define EPOLLERR 0x008
#endif
#ifndef EPOLLHUP
#define EPOLLHUP 0x010
#endif

#ifndef EPOLL_CTL_ADD
#define EPOLL_CTL_ADD 1
#define EPOLL_CTL_DEL 2
#define EPOLL_CTL_MOD 3
#endif

struct epoll_event {
    std::uint32_t events;
    union {
        int fd;
        void* ptr;
    } data;
};

namespace epoll_compat {

struct Instance {
    std::unordered_map<int, std::uint32_t> watches;
};

inline std::unordered_map<int, Instance>& instances() {
    static std::unordered_map<int, Instance> map;
    return map;
}

inline std::mutex& instances_mutex() {
    static std::mutex m;
    return m;
}

}  // namespace epoll_compat

inline int epoll_create1(int /*flags*/) {
    static std::atomic<int> next_fd{1024};
    const int fd = next_fd.fetch_add(1) + 1;
    std::lock_guard<std::mutex> lk(epoll_compat::instances_mutex());
    epoll_compat::instances()[fd] = {};
    return fd;
}

inline int epoll_ctl(int epfd, int op, int fd, struct epoll_event* event) {
    std::lock_guard<std::mutex> lk(epoll_compat::instances_mutex());
    auto& insts = epoll_compat::instances();
    const auto it = insts.find(epfd);
    if (it == insts.end()) {
        errno = EBADF;
        return -1;
    }
    auto& watches = it->second.watches;
    switch (op) {
        case EPOLL_CTL_ADD:
        case EPOLL_CTL_MOD:
            if (event == nullptr) {
                errno = EINVAL;
                return -1;
            }
            watches[fd] = event->events;
            return 0;
        case EPOLL_CTL_DEL:
            watches.erase(fd);
            return 0;
        default:
            errno = EINVAL;
            return -1;
    }
}

inline int epoll_wait(int epfd, struct epoll_event* events, int maxevents,
                      int timeout) {
    std::vector<pollfd> pfds;
    std::vector<int> fds;
    {
        std::lock_guard<std::mutex> lk(epoll_compat::instances_mutex());
        const auto& insts = epoll_compat::instances();
        const auto it = insts.find(epfd);
        if (it == insts.end()) {
            errno = EBADF;
            return -1;
        }
        const auto& watches = it->second.watches;
        pfds.reserve(watches.size());
        fds.reserve(watches.size());
        for (const auto& [fd, evs] : watches) {
            pfds.push_back({fd, static_cast<short>((evs & EPOLLIN ? POLLIN : 0) |
                                                   (evs & EPOLLOUT ? POLLOUT : 0)),
                            0});
            fds.push_back(fd);
        }
    }
    const int n = poll(pfds.data(), pfds.size(), timeout);
    if (n <= 0) {
        return n;
    }
    int count = 0;
    for (size_t i = 0; i < pfds.size() && count < maxevents; ++i) {
        std::uint32_t revents = 0;
        if (pfds[i].revents & POLLIN) {
            revents |= EPOLLIN;
        }
        if (pfds[i].revents & POLLOUT) {
            revents |= EPOLLOUT;
        }
        if (pfds[i].revents & (POLLHUP | POLLERR | POLLNVAL)) {
            revents |= EPOLLHUP;
        }
        if (revents != 0) {
            events[count].events = revents;
            events[count].data.fd = fds[i];
            ++count;
        }
    }
    return count;
}

#define COMPAT_MSG_NOSIGNAL 0
#define COMPAT_SET_NOSIGPIPE(fd)          \
    do {                                  \
        int one = 1;                      \
        setsockopt((fd), SOL_SOCKET,      \
                   SO_NOSIGPIPE, &one,    \
                   sizeof(one));          \
    } while (0)

#endif
