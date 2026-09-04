FROM gcc:13-bookworm

WORKDIR /work

COPY Makefile ./
COPY src/ ./src/
COPY tests/ ./tests/

RUN make clean && make && make test

CMD ["./polycache"]
