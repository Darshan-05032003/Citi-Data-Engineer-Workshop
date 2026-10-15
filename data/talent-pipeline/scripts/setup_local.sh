#!/usr/bin/env bash
# Idempotent local setup: .venv with Python 3.11, dev requirements, Java 17 hook in activate.
# Never installs system packages. Override the interpreter with PYTHON and the JDK with JAVA17_HOME.
set -euo pipefail

JOB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="$JOB_DIR/.venv"
PYTHON="${PYTHON:-python3.11}"

find_java17() {
    if [ -n "${JAVA17_HOME:-}" ]; then echo "$JAVA17_HOME"; return; fi
    for d in /usr/lib/jvm/java-17-openjdk*; do
        if [ -x "$d/bin/java" ]; then echo "$d"; return; fi
    done
    if [ -x /usr/libexec/java_home ]; then /usr/libexec/java_home -v 17 2>/dev/null && return; fi
    return 1
}

JAVA17="$(find_java17)" || {
    echo "ERROR: Java 17 not found. Install it (e.g. 'sudo apt install openjdk-17-jdk-headless'" >&2
    echo "or 'brew install openjdk@17') or set JAVA17_HOME, then re-run." >&2
    exit 1
}
if ! "$JAVA17/bin/java" -version 2>&1 | grep -q 'version "17'; then
    echo "ERROR: $JAVA17 is not Java 17" >&2
    exit 1
fi

if [ ! -x "$VENV/bin/python" ]; then
    command -v "$PYTHON" >/dev/null 2>&1 || { echo "ERROR: $PYTHON not found" >&2; exit 1; }
    echo "Creating $VENV with $PYTHON"
    "$PYTHON" -m venv "$VENV"
else
    echo "Using existing $VENV"
fi

"$VENV/bin/python" -m pip install -q --upgrade pip
"$VENV/bin/python" -m pip install -q -r "$JOB_DIR/requirements-dev.txt"

if ! grep -q "unset SPARK_HOME" "$VENV/bin/activate"; then
    cat >> "$VENV/bin/activate" <<EOF

# Java 17 for this project only (matches the cloud image); system Java is untouched
export JAVA_HOME=$JAVA17
export PATH="\$JAVA_HOME/bin:\$PATH"
# A SPARK_HOME for another Spark version breaks the pip-installed pyspark 3.5.0
unset SPARK_HOME
EOF
    echo "Added the JAVA_HOME / SPARK_HOME hook to .venv/bin/activate"
else
    echo "activate hook already present"
fi

PYSPARK_VERSION="$("$VENV/bin/python" -c 'import pyspark; print(pyspark.__version__)')"
[ "$PYSPARK_VERSION" = "3.5.0" ] || { echo "ERROR: pyspark $PYSPARK_VERSION, expected 3.5.0" >&2; exit 1; }
echo "OK Python: $("$VENV/bin/python" --version 2>&1)"
echo "OK Java 17: $JAVA17"
echo "OK pyspark: $PYSPARK_VERSION"
echo "Next: source .venv/bin/activate && python job.py"
