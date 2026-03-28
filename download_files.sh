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
AZURE_REMOTE_OUTPUT_DIR="${AZURE_REMOTE_OUTPUT_DIR:-/home/${AZURE_VM_USER}/output}"
LOCAL_OUTPUT_TARGET="${LOCAL_OUTPUT_TARGET:-src/}"

scp -i "${AZURE_SSH_KEY_PATH}" -rp "${AZURE_VM_USER}@${AZURE_VM_HOST}:${AZURE_REMOTE_OUTPUT_DIR}" "${LOCAL_OUTPUT_TARGET}"
