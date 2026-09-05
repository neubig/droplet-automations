#!/bin/bash
# Install the OpenHands SDK from PyPI into an isolated virtual environment.
#
# Each automation run gets its own venv in its work directory, ensuring:
# - No conflicts between concurrent automation runs
# - Clean isolation of dependencies
# - No pollution of the system Python environment
#
# The SDK version is fetched from the automation service API on every run so
# that deploying a new service version is the only step required to roll out a
# new SDK — no tarball re-generation or hardcoded version pins needed.
set -e

echo "[setup] Fetching SDK version from automation service"
PYTHON_JSON=python3
if ! command -v python3 >/dev/null 2>&1; then
    if command -v python >/dev/null 2>&1; then
        PYTHON_JSON=python
    elif command -v py >/dev/null 2>&1; then
        PYTHON_JSON='py -3'
    else
        echo "[setup] ERROR: python3, python, or py is required to parse SDK version" >&2
        exit 1
    fi
fi
set +e
SDK_VERSION=""
for attempt in 1 2 3 4 5; do
    SDK_VERSION=$(curl --retry 2 --retry-delay 2 -sf "${AUTOMATION_API_URL}/sdk-version" \
      | ${PYTHON_JSON} -c 'import sys, json; print(json.load(sys.stdin)["version"])' 2>/dev/null)
    if [ -n "$SDK_VERSION" ]; then break; fi
    echo "[setup] SDK version lookup failed (attempt $attempt/5); retrying" >&2
    sleep $((attempt * 2))
done
set -e
if [ -z "$SDK_VERSION" ]; then
    echo "[setup] ERROR: Failed to fetch SDK version from ${AUTOMATION_API_URL}/sdk-version" >&2
    exit 1
fi

echo "[setup] Creating isolated virtual environment"
# Pin >=3.12 so uv doesn't default to an older system Python (e.g. macOS
# CommandLineTools 3.9), which can't satisfy openhands-sdk's requires-python.
uv venv .venv --python '>=3.12' --quiet

echo "[setup] Installing OpenHands SDK from PyPI (version: $SDK_VERSION)"
uv pip install --quiet \
  "openhands-sdk==${SDK_VERSION}" \
  "openhands-tools==${SDK_VERSION}" \
  "openhands-workspace==${SDK_VERSION}"

echo "[setup] Done"
