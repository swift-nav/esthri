FROM rust:1.41-slim-buster

ARG DEBIAN_FRONTEND=noninterative

ARG SCCACHE_BIN=sccache-0.2.12-x86_64-unknown-linux-musl/sccache
ARG SCCACHE_URL=https://github.com/mozilla/sccache/releases/download/0.2.12/sccache-0.2.12-x86_64-unknown-linux-musl.tar.gz

ENV SCCACHE_CACHE_SIZE=100G
ENV SCCACHE_DIR=/opt/sccache

RUN mkdir -p $SCCACHE_DIR

RUN \
      apt-get update \
   && apt-get install -y libssl-dev pkg-config curl \
   && curl -sSL -o /tmp/sccache.tgz $SCCACHE_URL \
   && mkdir /tmp/sccache \
   && tar --strip-components=1 -C /tmp/sccache -xzf /tmp/sccache.tgz \
   && mv /tmp/sccache/sccache /usr/local/bin \
   && chmod +x /usr/local/bin/sccache \
   && rustup component add rustfmt --toolchain 1.41.0-x86_64-unknown-linux-gnu \
   && rustup component add clippy --toolchain 1.41.0-x86_64-unknown-linux-gnu \
   && rm -rf /var/lib/apt/lists/* \
   && rm -rf /tmp/sccache /tmp/sccache.tgz

ENV RUSTC_WRAPPER=/usr/local/bin/sccache
