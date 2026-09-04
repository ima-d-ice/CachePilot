CXX      := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread -MMD -MP
SRCS     := $(shell find src -name '*.cpp')
OBJS     := $(SRCS:.cpp=.o)
DEPS     := $(OBJS:.o=.d)
TARGET   := polycache
TESTS    := test_sieve test_protocol test_storage
SAN_FLAGS := -fsanitize=address,undefined -fno-omit-frame-pointer -g

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

test: $(TESTS)
	@for t in $(TESTS); do echo "== $$t =="; ./$$t; done

test_sieve: tests/test_sieve.cpp
	$(CXX) $(CXXFLAGS) -I src -o $@ tests/test_sieve.cpp src/eviction/sieve.cpp

test_protocol: tests/test_protocol.cpp
	$(CXX) $(CXXFLAGS) -I src -o $@ tests/test_protocol.cpp src/protocol.cpp

test_storage: tests/test_storage.cpp
	$(CXX) $(CXXFLAGS) -I src -o $@ tests/test_storage.cpp src/storage.cpp src/ttl.cpp src/eviction/lru.cpp src/eviction/lfu.cpp src/eviction/sieve.cpp

clean:
	rm -f $(OBJS) $(DEPS) $(TARGET) $(TESTS)

sanitize: clean
	$(MAKE) CXXFLAGS="$(CXXFLAGS) $(SAN_FLAGS)" $(TARGET) $(TESTS)
	@for t in $(TESTS); do echo "== $$t (san) =="; ./$$t; done

tidy:
	clang-tidy $(SRCS) -- -std=c++17 -I src

-include $(DEPS)

.PHONY: all clean test sanitize tidy
