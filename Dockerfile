FROM rust:1.93-alpine AS base
RUN rustup target add x86_64-unknown-linux-musl
RUN cargo install cargo-chef --version ^0.1
RUN apk add --no-cache \
    ca-certificates \
    curl \
    musl-dev \
    cmake \
    make \
    clang \
    clang-dev \
    llvm-dev \
    pkgconfig \
    openssl-dev \
    openssl-libs-static \
    libc-dev \
    gcc \
    g++ \
    meson \
    ninja \
    wget
WORKDIR /tmp/opus
RUN wget https://downloads.xiph.org/releases/opus/opus-1.6.1.tar.gz && \
    tar xzf opus-1.6.1.tar.gz && \
    cd opus-1.6.1 && \
    meson setup build \
        --prefix=/usr \
        --libdir=/usr/lib \
        --default-library=static \
        --buildtype=release && \
    meson compile -C build && \
    meson install -C build && \
    cd / && rm -rf /tmp/opus
ENV LIBCLANG_PATH=/usr/lib
ENV CMAKE_POLICY_VERSION_MINIMUM=3.5
ENV PKG_CONFIG_ALLOW_CROSS=1
ENV PKG_CONFIG_ALL_STATIC=1
ENV RUSTFLAGS="-C target-feature=+crt-static"

FROM base AS planner
WORKDIR /app
COPY . .
RUN rm rust-toolchain.toml
RUN cargo chef prepare --recipe-path recipe.json

FROM base AS builder
WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release -p terminal-games-server --target=x86_64-unknown-linux-musl --recipe-path recipe.json
COPY . .
RUN rm rust-toolchain.toml
RUN cargo build --release -p terminal-games-server --target=x86_64-unknown-linux-musl

FROM scratch
COPY --from=builder /etc/ssl/certs /etc/ssl/certs
COPY --from=builder /etc/ssl/cert.pem /etc/ssl/cert.pem
ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/terminal-games-server /terminal-games-server
COPY ./cmd/menu/menu.wasm ./examples/kitchen-sink/kitchen-sink.wasm .
ENTRYPOINT ["/terminal-games-server"]
