#!/usr/bin/env bash
# Load the workshop HR CSVs into PostgreSQL schema $POSTGRES_SCHEMA (default hr_source) for
# SOURCE_MODE=postgres. Idempotent: the schema is dropped and recreated, then the dataset's
# ORIGINAL load.sh (unedited) runs with search_path pointed at that schema.
#
# Env: POSTGRES_HOST (localhost), POSTGRES_PORT (5432), POSTGRES_NAME, POSTGRES_USER,
#      POSTGRES_PASS (required), POSTGRES_SCHEMA (hr_source), IS_LOCAL (true; false -> sslmode=require),
#      DATASET_DIR (default: the downloaded dataset, see scripts/fetch_dataset.sh).
# Note: the original load.sh passes the password inside psql's connection string (visible in
#       `ps` while it runs); this script itself only uses PGPASSWORD.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DATASET_DIR="${DATASET_DIR:-$REPO_ROOT/sampleData/data-engineering-sample-performance-scenario/data-engineering-performance-talent-management}"
SCHEMA="${POSTGRES_SCHEMA:-hr_source}"

for var in POSTGRES_NAME POSTGRES_USER POSTGRES_PASS; do
    if [ -z "${!var:-}" ]; then
        echo "ERROR: $var is not set" >&2
        exit 1
    fi
done
if ! [[ "$SCHEMA" =~ ^[a-z_][a-z0-9_]*$ ]]; then
    echo "ERROR: invalid POSTGRES_SCHEMA '$SCHEMA' (lowercase letters, digits, underscore)" >&2
    exit 1
fi
if [ ! -f "$DATASET_DIR/load.sh" ]; then
    echo "ERROR: $DATASET_DIR/load.sh not found; run scripts/fetch_dataset.sh first" >&2
    exit 1
fi

export POSTGRES_HOST="${POSTGRES_HOST:-localhost}" POSTGRES_PORT="${POSTGRES_PORT:-5432}"
export PGPASSWORD="$POSTGRES_PASS"
SSLMODE="prefer"
if [ "${IS_LOCAL:-true}" != "true" ]; then
    SSLMODE="require"
fi
CONN="host=$POSTGRES_HOST port=$POSTGRES_PORT dbname=$POSTGRES_NAME user=$POSTGRES_USER sslmode=$SSLMODE"

echo "Recreating schema $SCHEMA on $POSTGRES_HOST:$POSTGRES_PORT/$POSTGRES_NAME"
psql "$CONN" -q -v ON_ERROR_STOP=1 -c "DROP SCHEMA IF EXISTS $SCHEMA CASCADE; CREATE SCHEMA $SCHEMA;"

PGOPTIONS="-c search_path=$SCHEMA" bash "$DATASET_DIR/load.sh"

echo "Row counts in $SCHEMA:"
psql "$CONN" -At -F ' ' -v ON_ERROR_STOP=1 -c "
    SELECT 'teams', count(*) FROM $SCHEMA.teams
    UNION ALL SELECT 'competencies', count(*) FROM $SCHEMA.competencies
    UNION ALL SELECT 'employees', count(*) FROM $SCHEMA.employees
    UNION ALL SELECT 'projects', count(*) FROM $SCHEMA.projects
    UNION ALL SELECT 'performance_reviews', count(*) FROM $SCHEMA.performance_reviews
    UNION ALL SELECT 'competency_assessments', count(*) FROM $SCHEMA.competency_assessments
    UNION ALL SELECT 'talent_review', count(*) FROM $SCHEMA.talent_review"
