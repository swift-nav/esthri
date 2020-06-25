FROM rust:1.44-slim-stretch

ARG DEBIAN_FRONTEND=noninterative

ARG SCCACHE_URL=https://github.com/mozilla/sccache/releases/download/0.2.12/sccache-0.2.12-x86_64-unknown-linux-musl.tar.gz

ENV SCCACHE_CACHE_SIZE=100G
ENV SCCACHE_DIR=/opt/sccache

RUN mkdir -p $SCCACHE_DIR

RUN    echo "deb http://deb.debian.org/debian stretch-backports main non-free" >/etc/apt/sources.list.d/backports.list \
    && echo "deb-src http://httpredir.debian.org/debian stretch-backports main non-free" >>/etc/apt/sources.list.d/backports.list

RUN \
      apt-get update \
   && apt-get install -y libssl-dev pkg-config curl openssh-client git-lfs binutils \
   && rm -rf /var/lib/apt/lists/* \
   && curl -sSL -o /tmp/sccache.tgz $SCCACHE_URL \
   && mkdir /tmp/sccache \
   && tar --strip-components=1 -C /tmp/sccache -xzf /tmp/sccache.tgz \
   && mv /tmp/sccache/sccache /usr/local/bin \
   && chmod +x /usr/local/bin/sccache \
   && rm -rf /tmp/sccache /tmp/sccache.tgz \
   && rustup component add rustfmt \
   && rustup component add clippy

ENV RUSTC_WRAPPER=/usr/local/bin/sccache

## Set-up Jenkins user
RUN useradd -u 1001 -ms /bin/bash -G staff jenkins
