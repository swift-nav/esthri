ARG RUST_VERSION=1.72.0

FROM rust:${RUST_VERSION}-slim-buster

ARG DEBIAN_FRONTEND=noninterative

ARG SCCACHE_BASE=https://github.com/mozilla/sccache/releases/download
ARG SCCACHE_VER=v0.2.15
ARG SCCACHE_URL=${SCCACHE_BASE}/${SCCACHE_VER}/sccache-${SCCACHE_VER}-x86_64-unknown-linux-musl.tar.gz

ENV SCCACHE_CACHE_SIZE=100G
ENV SCCACHE_DIR=/opt/sccache

RUN mkdir -p $SCCACHE_DIR

ARG CARGO_MAKE_VER=0.33.0
ARG CARGO_MAKE_DIR=cargo-make-v${CARGO_MAKE_VER}-x86_64-unknown-linux-musl
ARG CARGO_MAKE_FILE=${CARGO_MAKE_DIR}.zip
ARG CARGO_MAKE_URL_BASE=https://github.com/sagiegurari/cargo-make/releases/download/
ARG CARGO_MAKE_URL=${CARGO_MAKE_URL_BASE}/${CARGO_MAKE_VER}/${CARGO_MAKE_FILE}

RUN \
      apt-get update \
   && apt-get install -y libssl-dev pkg-config curl openssh-client git-lfs binutils musl-dev musl-tools unzip \
   && rm -rf /var/lib/apt/lists/* \
   && curl -sSL -o /tmp/sccache.tgz $SCCACHE_URL \
   && mkdir /tmp/sccache \
   && tar --strip-components=1 -C /tmp/sccache -xzf /tmp/sccache.tgz \
   && mv /tmp/sccache/sccache /usr/local/bin \
   && chmod +x /usr/local/bin/sccache \
   && rm -rf /tmp/sccache /tmp/sccache.tgz \
   && rustup target add x86_64-unknown-linux-musl \
   && rustup component add rustfmt \
   && rustup component add clippy \
   && curl -sSLO ${CARGO_MAKE_URL} \
   && unzip ${CARGO_MAKE_FILE} \
   && mv ${CARGO_MAKE_DIR}/cargo-make /usr/local/bin \
   && chmod +x /usr/local/bin/cargo-make \
   && rm -rf ${CARGO_MAKE_DIR} ${CARGO_MAKE_FILE}

ENV RUSTC_WRAPPER=/usr/local/bin/sccache

## Set-up Jenkins user
RUN useradd -u 1001 -ms /bin/bash -G staff jenkins
