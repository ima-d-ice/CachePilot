#include "aof.h"
#include "admin.h"
#include "server.h"
#include "storage.h"

#include <csignal>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

using namespace std;

namespace {

Server* g_server = nullptr;
AdminServer* g_admin = nullptr;

void handle_signal(int /*sig*/) {
    if (g_server != nullptr) {
        g_server->stop();
    }
    if (g_admin != nullptr) {
        g_admin->stop();
    }
}

void print_usage(const char* prog) {
    cerr << "usage: " << prog
         << " [--port PORT] [--admin-port PORT] [--memory-limit MB] "
             "[--aof-file FILE] [--no-aof]\n";
}

}  // namespace

int main(int argc, char** argv) {  // NOLINT(bugprone-exception-escape): outermost frame; std::exceptions are caught below, anything else terminating here is the defined behavior
    int port = 6379;
    int admin_port = 8080;
    size_t memory_mb = 64;
    string aof_file = "polycache.aof";
    bool use_aof = true;

    for (int i = 1; i < argc; ++i) {
        const string arg = argv[i];
        auto next_value = [&](const string& name) -> string {
            if (i + 1 >= argc) {
                cerr << "missing value for " << name << "\n";
                exit(1);
            }
            return argv[++i];
        };
        try {
            if (arg == "--port") {
                port = stoi(next_value(arg));
            } else if (arg == "--admin-port") {
                admin_port = stoi(next_value(arg));
            } else if (arg == "--memory-limit") {
                memory_mb = static_cast<size_t>(stoul(next_value(arg)));
            } else if (arg == "--aof-file") {
                aof_file = next_value(arg);
            } else if (arg == "--no-aof") {
                use_aof = false;
            } else if (arg == "--help" || arg == "-h") {
                print_usage(argv[0]);
                return 0;
            } else {
                cerr << "unknown option: " << arg << "\n";
                print_usage(argv[0]);
                return 1;
            }
        } catch (const exception&) {
            cerr << "invalid numeric value for " << arg << "\n";
            return 1;
        }
    }

    // Startup can throw (TTL/admin thread creation, allocations): report
    // cleanly instead of escaping main with an uncaught exception.
    try {
    Storage storage(memory_mb * 1024 * 1024);
    std::unique_ptr<AOFLogger> aof;
    if (use_aof) {
        aof = std::make_unique<AOFLogger>(aof_file);
        aof->replay(&storage);
    }

    Server server(port, &storage, aof.get());
    AdminServer admin(admin_port, &storage);

    struct sigaction sa {};
    sa.sa_handler = handle_signal;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    g_admin = &admin;
        thread admin_thread([&admin] { admin.start(); });

        g_server = &server;
        cout << "PolyCache listening on port " << port << ", admin on port "
             << admin_port << ", policies: lru, lfu, sieve" << '\n';
        server.start();
        g_server = nullptr;

        admin.stop();
        admin_thread.join();
    } catch (const exception& e) {
        cerr << "fatal: " << e.what() << "\n";
        return 1;
    }
    g_admin = nullptr;

    return 0;
}