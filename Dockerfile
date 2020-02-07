FROM rust:1.41-slim-buster

ARG DEBIAN_FRONTEND=noninterative

RUN \
      apt-get update \
   && apt-get install -y libssl-dev pkg-config \
   && rm -rf /var/lib/apt/lists/*

RUN rustup component add clippy --toolchain 1.41.0-x86_64-unknown-linux-gnu
