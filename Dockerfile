FROM rust:1.61-slim-buster AS builder

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y libssl-dev pkg-config cmake build-essential && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/apibara

# Build dependencies first for better caching.
RUN cargo new --lib --vcs none apibara
RUN cargo new --lib --vcs none cli
RUN cargo new --lib --vcs none starknet-rpc
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock

COPY apibara/Cargo.toml apibara/Cargo.toml
COPY cli/Cargo.toml cli/Cargo.toml
COPY starknet-rpc/Cargo.toml starknet-rpc/Cargo.toml

RUN CARGO_INCREMENTAL=0 DEPENDENCY_LAYER=1 cargo build --release -p apibara

# Compile apibara now
COPY . .

# Force recompilation of everything
RUN touch apibara/src/lib.rs
RUN touch cli/src/main.rs

RUN CARGO_INCREMENTAL=0 DEPENDENCY_LAYER=1 cargo build --release -p apibara-cli

FROM debian:buster-slim AS runner

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd --gid 1000 apibara && \
    useradd --no-log-init --uid 1000 --gid apibara --no-create-home apibara

COPY --from=builder /usr/src/apibara/target/release/apibara-cli /usr/local/bin/apibara-cli

# Create volume for user to put their configuration
VOLUME /usr/etc/apibara

USER 1000:1000
EXPOSE 7171

ENTRYPOINT [ "/usr/local/bin/apibara-cli" ]
