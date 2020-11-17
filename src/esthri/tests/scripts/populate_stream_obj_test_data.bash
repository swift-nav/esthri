#!/usr/bin/env bash

set -ex

TEST_DIR=/tmp/esthri-test
mkdir -p $TEST_DIR

# https://unix.stackexchange.com/questions/199863/create-many-files-with-random-content
for n in {1..1010}; do
    dd if=/dev/urandom of="${TEST_DIR}/file$(printf %03d "$n").bin" bs=1 count=$((RANDOM + 1024))
done

aws s3 sync $TEST_DIR s3://esthri-test/test_handle_stream_objects/

du -h $TEST_DIR

rm -rf $TEST_DIR
