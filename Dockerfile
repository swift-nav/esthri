FROM rust:1.41-slim-buster

ARG DEBIAN_FRONTEND=noninterative

RUN \
      apt-get update \
   && apt-get install -y libssl-dev \
   && rm -rf /var/lib/apt/lists/*
