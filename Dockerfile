FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto proto
COPY src src
RUN cargo build --release --locked

FROM debian:bookworm-slim
# util-linux provides mount/umount for the CSI node plugin.
RUN apt-get update && apt-get install -y --no-install-recommends util-linux ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/k8s-jetstream-ufs /usr/local/bin/k8s-jetstream-ufs
ENTRYPOINT ["/usr/local/bin/k8s-jetstream-ufs"]
