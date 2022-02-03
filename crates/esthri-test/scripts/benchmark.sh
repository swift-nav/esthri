#!/usr/bin/env bash

# Short script to benchmark the memory and bandwidth performance of esthri over time

set -ex


export RUST_LOG=warn

mkdir -p /tmp/esthri_results/

if [[ $1 == esthri_new ]]; then
    binary=/tmp/esthri
else if [[ $1 == esthri_old ]]; then
    binary=esthri
else if [[ $1 == aws ]]; then
    binary=aws.real
else
    exit 1
fi
fi
fi

if [[ $2 == sync_up ]]; then 
    command="sync --destination s3://esthri-test/sam/$(uuidgen) --source /tmp/test_files "
    byte_count="/sys/class/net/eth0/statistics/tx_bytes"
else if [[ $2 == sync_down ]]; then
    down_dir=$(mktemp -d)
    command="sync --source s3://esthri-test/sam/testing2/ --destination $down_dir "
    byte_count="/sys/class/net/eth0/statistics/rx_bytes"
else if [[ $2 == aws_sync_up ]]; then
    command="s3 sync /tmp/test_files s3://esthri-test/sam/$(uuidgen)"
    byte_count="/sys/class/net/eth0/statistics/tx_bytes"
else if [[ $2 == aws_sync_down ]]; then
    down_dir=$(mktemp -d)
    command="s3 sync s3://esthri-test/sam/testing2/ $down_dir "
    byte_count="/sys/class/net/eth0/statistics/rx_bytes"
else
    exit 1
fi
fi
fi
fi

if [[ $3 == compress ]]; then 
    command+="--transparent-compression "
fi

if [[ $4 == new_settings ]]; then
    export ESTHRI_CONCURRENT_DOWNLOADER_TASKS=32
    export ESTHRI_CONCURRENT_UPLOAD_TASKS=32
    export ESTHRI_CONCURRENT_SYNC_TASKS=8
fi

$binary $command &

pid=$!
echo $pid
logfile=/tmp/esthri_results/$1_$2_$3_$4
start=$(date +%s%N)
bytes_start=$(cat $byte_count)

while mem=$(ps -o rss= -p "$pid"); do
    time=$(date +%s%N)

    bytes=$(cat $byte_count)

    printf "%d %s %d\n" $((time-start)) "$mem" $((bytes-bytes_start)) >> "$logfile"

    sleep .01

    set +x
done

rm -rf $down_dir

printf "$logfile"