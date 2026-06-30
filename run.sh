#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="kraken-ws-tests"

if [[ -z "$(docker images -q "${IMAGE_NAME}" 2>/dev/null)" ]]; then
  echo "Image '${IMAGE_NAME}' not found locally — building..."
  docker build -t "${IMAGE_NAME}" .
else
  echo "Found existing image '${IMAGE_NAME}' — skipping build."
fi

docker run --rm "${IMAGE_NAME}"
