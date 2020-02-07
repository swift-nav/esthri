FROM rust:1.41-slim-buster

ARG DEBIAN_FRONTEND=noninterative

ARG SCCACHE_BIN=sccache-0.2.12-x86_64-unknown-linux-musl/sccache
ARG SCCACHE_URL=https://github.com/mozilla/sccache/releases/download/0.2.12/sccache-0.2.12-x86_64-unknown-linux-musl.tar.gz

ENV SCCACHE_CACHE_SIZE=100G
ENV SCCACHE_DIR=/sccache

RUN \
      apt-get update \
   && apt-get install -y libssl-dev pkg-config curl \
   && curl -sSL -O /tmp/sccache.tgz $SCCACHE_URL \
   && echo $SCCACHE_BIN | tar -T- --strip-components=1 -C /usr/local/bin /tmp/sccache.tgz \
   && rustup component add rustfmt --toolchain 1.41.0-x86_64-unknown-linux-gnu \
   && rustup component add clippy --toolchain 1.41.0-x86_64-unknown-linux-gnu \
   && mkdir /sccache
   && rm -rf /var/lib/apt/lists/* \
   && rm /tmp/sccache.tgz

