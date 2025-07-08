#!/usr/bin/env bash

ENV_FILE=".env.local"

if [ ! -f "$ENV_FILE" ]; then
  echo "⚠️  File '$ENV_FILE' does not exist." >&2
  return 1
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

echo "✅ Loaded environment variables from '$ENV_FILE'"