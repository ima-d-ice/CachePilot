#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace protocol {

struct Command {
    enum Type : std::uint8_t { SET, GET, DEL, METRICS, SWITCH_POLICY, PING, SELECT, UNKNOWN };
    Type type = UNKNOWN;
    std::vector<std::string> args;
};

// RESP2 frame parser (the Redis wire protocol). Tries to parse exactly one
// command from the front of `buf`. On success returns the Command and sets
// `consumed` to the number of bytes used. Returns nullopt (and leaves
// `consumed` untouched) if the buffer does not yet hold a complete frame —
// call again when more data arrives. A frame is "*<count>\r\n" followed by
// count bulk strings "$<len>\r\n<data>\r\n"; elements are copied verbatim
// (binary-safe). Safety: caps argument count and bulk length to prevent DoS
// via giant allocations or size_t overflow.
std::optional<Command> try_parse(const std::string& buf, std::size_t& consumed);

}  // namespace protocol