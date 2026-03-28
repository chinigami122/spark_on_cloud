#!/usr/bin/env bash
set -euo pipefail

if [ -f .env ]; then
	set -a
	. ./.env
	set +a
fi

: "${AZURE_VM_HOST:?AZURE_VM_HOST is required in .env}"
: "${AZURE_VM_USER:?AZURE_VM_USER is required in .env}"

AZURE_SSH_KEY_PATH="${AZURE_SSH_KEY_PATH:-spark-cluster_key.pem}"
AZURE_REMOTE_BASE_DIR="${AZURE_REMOTE_BASE_DIR:-/home/${AZURE_VM_USER}}"

scp -i "${AZURE_SSH_KEY_PATH}" -r ./src/* "${AZURE_VM_USER}@${AZURE_VM_HOST}:${AZURE_REMOTE_BASE_DIR}"