#!/usr/bin/env bash
set -euo pipefail

helm repo add strimzi https://strimzi.io/charts/
helm repo update strimzi

helm upgrade --install strimzi-cluster-operator strimzi/strimzi-kafka-operator \
  --namespace streaming \
  --set watchAnyNamespace=false

kubectl rollout status deployment/strimzi-cluster-operator -n streaming --timeout=180s
