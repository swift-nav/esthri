#!/usr/bin/env bash

set -ex 

kubectl cp crates/esthri-test/scripts/benchmark.sh  sitl-orch-fan-out-5dcc69f459-qvfc2:/tmp/benchmark.sh
kubectl cp target/release/esthri  sitl-orch-fan-out-5dcc69f459-qvfc2:/tmp/esthri
kubectl exec -it  sitl-orch-fan-out-5dcc69f459-qvfc2 --  rm -rf /tmp/esthri_*

kubectl exec -it  sitl-orch-fan-out-5dcc69f459-qvfc2 -- /tmp/benchmark.sh esthri_new sync_up uncompressed default_settings
kubectl exec -it  sitl-orch-fan-out-5dcc69f459-qvfc2 -- /tmp/benchmark.sh esthri_new sync_up compress default_settings
kubectl exec -it  sitl-orch-fan-out-5dcc69f459-qvfc2 -- /tmp/benchmark.sh esthri_new sync_down uncompressed default_settings
kubectl exec -it  sitl-orch-fan-out-5dcc69f459-qvfc2 -- /tmp/benchmark.sh esthri_new sync_down compress default_settings

kubectl exec -it  sitl-orch-fan-out-5dcc69f459-qvfc2 -- /tmp/benchmark.sh esthri_old sync_up uncompressed default_settings
kubectl exec -it  sitl-orch-fan-out-5dcc69f459-qvfc2 -- /tmp/benchmark.sh esthri_old sync_up compress default_settings
kubectl exec -it  sitl-orch-fan-out-5dcc69f459-qvfc2 -- /tmp/benchmark.sh esthri_old sync_down uncompressed default_settings
kubectl exec -it  sitl-orch-fan-out-5dcc69f459-qvfc2 -- /tmp/benchmark.sh esthri_old sync_down compress default_settings

kubectl exec -it  sitl-orch-fan-out-5dcc69f459-qvfc2 -- /tmp/benchmark.sh esthri_new sync_up uncompressed new_settings
kubectl exec -it  sitl-orch-fan-out-5dcc69f459-qvfc2 -- /tmp/benchmark.sh esthri_new sync_up compress new_settings
kubectl exec -it  sitl-orch-fan-out-5dcc69f459-qvfc2 -- /tmp/benchmark.sh esthri_new sync_down uncompressed new_settings
kubectl exec -it  sitl-orch-fan-out-5dcc69f459-qvfc2 -- /tmp/benchmark.sh esthri_new sync_down compress new_settings

kubectl exec -it  sitl-orch-fan-out-5dcc69f459-qvfc2 -- /tmp/benchmark.sh aws aws_sync_up uncompressed default_settings
kubectl exec -it  sitl-orch-fan-out-5dcc69f459-qvfc2 -- /tmp/benchmark.sh aws aws_sync_down uncompressed default_settings
dir=investigations/$(uuidgen)
mkdir -p $dir
kubectl cp sitl-orch-fan-out-5dcc69f459-qvfc2:/tmp/esthri_results/. $dir

