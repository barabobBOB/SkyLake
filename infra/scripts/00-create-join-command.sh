#!/usr/bin/env bash
set -euo pipefail

sudo kubeadm token create --print-join-command
