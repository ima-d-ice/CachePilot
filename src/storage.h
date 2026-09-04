#pragma once

#include "eviction/policy.h"
#include "third_party/nlohmann/json.hpp"
#include "ttl.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

// Per-key metadata that survives across policy switches so a rebuild can
// preserve the eviction frontier instead of scattering it in hash order.
struct KeyMeta {
    std::string value;
    std::size_t size = 0;                 // value.size(), cached for eviction sizing
    uint64_t insert_seq = 0;              // global order of first insertion
    uint64_t last_access_seq = 0;         // global order of last set/get (recency)
    std::size_t freq = 0;                 // access count (for LFU)
    bool visited = false;                 // SIEVE visited bit
};

class Storage {
public:
    explicit Storage(std::size_t memory_limit = std::size_t{64} * 1024 * 1024);
    ~Storage() = default;

    void set(const std::string& key, const std::string& value, int ttl_sec);
    std::optional<std::string> get(const std::string& key);
    bool del(const std::string& key);
    bool switch_policy(const std::string& name);
    nlohmann::json metrics() const;

private:
    void evict_if_over_limit();
    void expire_key(const std::string& key);

    std::unordered_map<std::string, KeyMeta> data_;
    std::unique_ptr<EvictionPolicy> policy_;
    mutable std::mutex lock_;
    TTLManager ttl_;
    std::size_t hits_ = 0;
    std::size_t misses_ = 0;
    std::size_t evictions_ = 0;
    std::string policy_name_ = "lru";
    std::size_t memory_limit_;
    uint64_t access_seq_ = 0;             // monotonic counter for recency/frequency
};
