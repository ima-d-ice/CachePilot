#include "storage.h"

#include "eviction/lfu.h"
#include "eviction/lru.h"
#include "eviction/sieve.h"

#include <algorithm>
#include <cctype>
#include <vector>

using namespace std;

Storage::Storage(size_t memory_limit)
    : policy_(make_unique<LRU>()),
      ttl_([this](const string& key) { expire_key(key); }),
      memory_limit_(memory_limit) {}

void Storage::set(const string& key, const string& value, int ttl_sec) {
    lock_guard<mutex> lk(lock_);
    auto& meta = data_[key];
    const bool is_new = (meta.insert_seq == 0);
    if (is_new) {
        meta.insert_seq = ++access_seq_;
        meta.freq = 0;
        meta.visited = false;
    }
    meta.value = value;
    meta.size = value.size();
    meta.last_access_seq = ++access_seq_;
    ++meta.freq;
    policy_->add(key, meta.size);
    if (ttl_sec > 0) {
        ttl_.set_ttl(key, chrono::seconds(ttl_sec));
    } else {
        ttl_.erase(key);
    }
    evict_if_over_limit();
}

optional<string> Storage::get(const string& key) {
    lock_guard<mutex> lk(lock_);
    if (ttl_.is_expired(key)) {
        data_.erase(key);
        policy_->remove(key);
        ++misses_;
        return nullopt;
    }
    auto it = data_.find(key);
    if (it == data_.end()) {
        ++misses_;
        return nullopt;
    }
    KeyMeta& meta = it->second;
    meta.last_access_seq = ++access_seq_;
    ++meta.freq;
    meta.visited = true;          // a hit marks the key visited (SIEVE)
    policy_->touch(key);
    ++hits_;
    return meta.value;
}

bool Storage::del(const string& key) {
    lock_guard<mutex> lk(lock_);
    auto it = data_.find(key);
    if (it == data_.end()) {
        return false;
    }
    data_.erase(it);
    policy_->remove(key);
    ttl_.erase(key);
    return true;
}

bool Storage::switch_policy(const string& name) {
    lock_guard<mutex> lk(lock_);
    string lower = name;
    transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) {
        return static_cast<char>(tolower(c));
    });
    unique_ptr<EvictionPolicy> next;
    if (lower == "lru") {
        next = make_unique<LRU>();
    } else if (lower == "lfu") {
        next = make_unique<LFU>();
    } else if (lower == "sieve") {
        next = make_unique<SIEVE>();
    } else {
        return false;
    }

    // Collect resident keys with their metadata, then rebuild the new policy
    // in a deterministic order that preserves the eviction frontier. The old
    // hash-random iteration (for (auto& [k,v] : data_)) scrambled the frontier
    // and was the tax that made every switch a net loss.
    vector<pair<const string*, KeyMeta*>> ordered;
    ordered.reserve(data_.size());
    for (auto& [key, meta] : data_) {
        ordered.emplace_back(&key, &meta);
    }

    if (lower == "lru") {
        // Most-recently-used first => the rebuilt list is correctly ordered
        // (LRU evicts from the back = least recent).
        sort(ordered.begin(), ordered.end(),
             [](const auto& a, const auto& b) {
                 return a.second->last_access_seq > b.second->last_access_seq;
             });
        for (auto& [key, meta] : ordered) {
            next->add(*key, meta->size);
        }
    } else if (lower == "lfu") {
        // Lowest frequency first, recency tiebreak => buckets come out ordered.
        sort(ordered.begin(), ordered.end(),
              [](const auto& a, const auto& b) {
                  if (a.second->freq != b.second->freq) {
                      return a.second->freq < b.second->freq;
                  }
                  return a.second->last_access_seq < b.second->last_access_seq;
              });
        for (auto& [key, meta] : ordered) {
            next->add(*key, meta->size);
        }
    } else { // sieve
        // Recency order, coldest first. SIEVE evicts from the tail backward
        // sweeping unvisited keys; with all visited bits cold after a rebuild,
        // the tail is evicted first, so the least-recently-used keys must be at
        // the tail. SIEVE.add() puts each new key at the head, so adding
        // coldest-first leaves the coldest at the tail and the hottest at the
        // head (furthest from the hand). Cold-start the visited bits.
        sort(ordered.begin(), ordered.end(),
             [](const auto& a, const auto& b) {
                 return a.second->last_access_seq < b.second->last_access_seq;
             });
        for (auto& [key, meta] : ordered) {
            next->add(*key, meta->size);
        }
        // Restore the visited-bit frontier: a SIEVE rebuild cold-starts every
        // visited bit to false, which makes every key immediately evictable
        // and destroys the "recently accessed are protected" invariant. SIEVE
        // evicts from the tail backward skipping visited keys, so replay the
        // recently-accessed (visited) keys now that the hot ones sit near the
        // head. This is a no-op for LRU/LFU (their touch() reorders, so the
        // replay is skipped for them).
        for (auto& [key, meta] : ordered) {
            if (meta->visited) {
                next->touch(*key);
            }
        }
    }

    policy_ = std::move(next);
    policy_name_ = lower;
    return true;
}

nlohmann::json Storage::metrics() const {
    lock_guard<mutex> lk(lock_);
    nlohmann::json j;
    j["total_keys"] = static_cast<int>(data_.size());
    j["memory_bytes"] = static_cast<int>(policy_->memory_used());
    j["policy"] = policy_name_;
    j["hits"] = static_cast<int>(hits_);
    j["misses"] = static_cast<int>(misses_);
    j["evictions"] = static_cast<int>(evictions_);
    const auto total = static_cast<float>(hits_ + misses_);
    j["hit_rate"] = total > 0.0F ? static_cast<float>(hits_) / total : 0.0F;
    j["miss_rate"] = total > 0.0F ? static_cast<float>(misses_) / total : 0.0F;
    return j;
}

void Storage::evict_if_over_limit() {
    while (policy_->memory_used() > memory_limit_) {
        const string victim = policy_->evict();
        if (victim.empty()) {
            break;
        }
        data_.erase(victim);
        ttl_.erase(victim);
        ++evictions_;
    }
}

void Storage::expire_key(const string& key) {
    lock_guard<mutex> lk(lock_);
    data_.erase(key);
    policy_->remove(key);
}
