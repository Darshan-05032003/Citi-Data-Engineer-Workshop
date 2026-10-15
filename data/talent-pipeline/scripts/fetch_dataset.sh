#!/usr/bin/env bash
# Download the workshop dataset, verify its sha256 and unzip it to the default SOURCE_DIR.
# Skips the download if the dataset is already present. Override the target root with DATASET_ROOT.
set -euo pipefail

URL="http://www.codingworkshop.net/data/data-engineering-sample-performance-scenario.zip"
SHA256="9c400617fc047583e765345f0b2489fa7f0bf30e759d432b3cc7e80f3625a0c8"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
TARGET="${DATASET_ROOT:-$REPO_ROOT/sampleData}/data-engineering-sample-performance-scenario"
SOURCE_DIR="$TARGET/data-engineering-performance-talent-management"

if [ -f "$SOURCE_DIR/employees.csv" ] && [ -d "$SOURCE_DIR/learning_activity" ]; then
    echo "Dataset already present, skipping: $SOURCE_DIR"
    exit 0
fi

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
    else shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
ZIP="$TMP/dataset.zip"

echo "Downloading $URL"
if ! curl -fSL --retry 2 --max-time 300 -o "$ZIP" "$URL"; then
    echo "ERROR: download failed (blocked or offline)." >&2
    echo "Download it manually from $URL and unzip it into $TARGET/" >&2
    exit 1
fi

ACTUAL="$(sha256_of "$ZIP")"
if [ "$ACTUAL" != "$SHA256" ]; then
    echo "ERROR: checksum mismatch: expected $SHA256, got $ACTUAL" >&2
    exit 1
fi
echo "Checksum OK ($SHA256)"

mkdir -p "$TARGET"
unzip -q -o "$ZIP" -d "$TARGET"
echo "Unzipped to $SOURCE_DIR"
echo "CSV files: $(find "$SOURCE_DIR" -maxdepth 1 -name '*.csv' | wc -l | tr -d ' ')," \
     "JSONL files: $(find "$SOURCE_DIR/learning_activity" -name '*.jsonl' | wc -l | tr -d ' ')"
