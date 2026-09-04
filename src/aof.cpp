#include "aof.h"

#include "storage.h"
#include "third_party/nlohmann/json.hpp"

#include <fstream>
#include <string>

using namespace std;

AOFLogger::AOFLogger(const string& filename)
    : filename_(filename),
      file_(filename, ios::out | ios::app) {}

void AOFLogger::append_line(const string& line) {
    file_ << line << '\n';
    file_.flush();
}

void AOFLogger::log_set(const string& key, const string& value, int ttl) {
    nlohmann::json j;
    j["cmd"] = "SET";
    j["key"] = key;
    j["value"] = value;
    j["ttl"] = ttl;
    append_line(j.dump());
}

void AOFLogger::log_del(const string& key) {
    nlohmann::json j;
    j["cmd"] = "DEL";
    j["key"] = key;
    append_line(j.dump());
}

void AOFLogger::replay(Storage* storage) {
    if (storage == nullptr) {
        return;
    }
    file_.close();
    ifstream in(filename_);
    string line;
    while (getline(in, line)) {
        if (line.empty()) {
            continue;
        }
        try {
            const auto j = nlohmann::json::parse(line);
            const string cmd = j.value("cmd", "");
            if (cmd == "SET") {
                storage->set(j.value("key", ""), j.value("value", ""),
                             j.value("ttl", 0));
            } else if (cmd == "DEL") {
                storage->del(j.value("key", ""));
            }
        } catch (const nlohmann::json::parse_error&) {  // NOLINT(bugprone-empty-catch): torn tail line is skipped so the rest of the log still replays
        }
    }
    file_.open(filename_, ios::out | ios::app);
}
