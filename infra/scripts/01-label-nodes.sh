#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <second-node-name>" >&2
  exit 1
fi

SECOND_NODE="$1"

kubectl label node lena-cloud node-pool=streaming-processing --overwrite
kubectl label node lena-cloud storage=local-nvme --overwrite

kubectl label node "$SECOND_NODE" node-pool=lakehouse-query --overwrite
kubectl label node "$SECOND_NODE" storage=local-disk --overwrite

kubectl get nodes --show-labels
