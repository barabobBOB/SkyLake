#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

kubectl apply -f "$ROOT_DIR/infra/k8s/namespaces/namespaces.yaml"
kubectl apply -f "$ROOT_DIR/infra/k8s/lakehouse/minio.yaml"
kubectl apply -f "$ROOT_DIR/infra/k8s/lakehouse/iceberg-rest.yaml"
kubectl apply -f "$ROOT_DIR/infra/k8s/lakehouse/trino.yaml"
kubectl apply -f "$ROOT_DIR/infra/k8s/kafka/kafka-single-node.yaml"

kubectl get pods -n streaming
kubectl get pods -n lakehouse
