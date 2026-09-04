FROM rust:1.98-bookworm AS builder
WORKDIR /src
COPY . .
RUN cargo build --release -p ketebe-mcp

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/ketebe-mcp /usr/local/bin/ketebe-mcp
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/ketebe-mcp"]
