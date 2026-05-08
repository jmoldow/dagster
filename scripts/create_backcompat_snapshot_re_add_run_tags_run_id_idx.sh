#!/usr/bin/env bash
# create_backcompat_snapshot_re_add_run_tags_run_id_idx.sh
#
# Creates backcompat DB snapshots for migration 051 (09e0cfebad78,
# "re-add run_tags_run_id index to schema"), for both PostgreSQL and MySQL.
#
# The snapshots represent the DB state BEFORE migration 051 runs:
#   - alembic_version = 29b539ebc72a  (migration 050, the parent of 051)
#   - run_tags table has idx_run_tags(key, value)     -- the OLD slow index
#   - run_tags table does NOT have idx_run_tags_run_id -- the new fast index
#
# HOW TO USE THIS SCRIPT (follow the README instructions exactly):
#
#   Per python_modules/dagster/dagster/_core/storage/alembic/README.md, snapshots
#   must be created from a code branch that does NOT yet contain the migration being
#   tested (migration 051 / 09e0cfebad78).  The script runs `dagster instance migrate`
#   which will only apply migrations that exist in the current source tree, so running
#   from a pre-051 commit naturally produces a DB at alembic revision 29b539ebc72a.
#
#   Steps:
#     1. Switch to the commit just BEFORE migration 051 was added:
#          git checkout <commit-before-051>
#        (or use a branch that does not contain 051 -- verify with:
#          ls python_modules/dagster/dagster/_core/storage/alembic/versions/ | grep 09e0cfebad78
#        -- the file should NOT exist)
#
#     2. Set up the Python venv for this older commit:
#          uv venv --python 3.12
#          source .venv/bin/activate
#          make dev_install
#
#     3. Run this script:
#          ./scripts/create_backcompat_snapshot_re_add_run_tags_run_id_idx.sh
#
#     4. Switch back to your working branch:
#          git checkout -
#
#     5. Verify the dump files and run the backcompat tests (instructions printed at
#        end of this script).
#
# Usage:
#   # Both databases (default):
#   ./scripts/create_backcompat_snapshot_re_add_run_tags_run_id_idx.sh
#
#   # Only postgres:
#   ./scripts/create_backcompat_snapshot_re_add_run_tags_run_id_idx.sh --postgres-only
#
#   # Only mysql:
#   ./scripts/create_backcompat_snapshot_re_add_run_tags_run_id_idx.sh --mysql-only
#
# Requirements:
#   - Docker (for docker compose; psql/pg_dump/mysql/mysqldump run inside containers)
#   - Python venv with dagster, dagster-postgres, dagster-mysql installed
#     Set up with: uv venv --python 3.12 && source .venv/bin/activate && make dev_install
#
# Run from the repository root.

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

SCRIPT_NAME="$(basename "$0")"

# Guard: abort if migration 051 (09e0cfebad78) already exists in the source tree.
# The script must be run from a commit that predates migration 051 so that
# `dagster instance migrate` naturally stops at revision 29b539ebc72a.
MIGRATION_051="python_modules/dagster/dagster/_core/storage/alembic/versions/051_09e0cfebad78_re_add_run_tags_run_id_index_to_schema.py"
if [ -f "$MIGRATION_051" ]; then
    echo "[$SCRIPT_NAME] ERROR: Migration 051 (09e0cfebad78) already exists at:" >&2
    echo "  $MIGRATION_051" >&2
    echo "" >&2
    echo "  This script must be run from a git commit that predates migration 051." >&2
    echo "  Check out the commit just before 051 was added, reinstall the venv" >&2
    echo "  (uv venv --python 3.12 && source .venv/bin/activate && make dev_install)," >&2
    echo "  then re-run this script." >&2
    exit 1
fi

# ──────────────────────────────────────────────────────────────────────────────
# Argument parsing
# ──────────────────────────────────────────────────────────────────────────────
DO_POSTGRES=1
DO_MYSQL=1

for arg in "$@"; do
    case "$arg" in
        --postgres-only) DO_MYSQL=0 ;;
        --mysql-only)    DO_POSTGRES=0 ;;
        --help|-h)
            grep '^#' "$0" | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            echo "Usage: $SCRIPT_NAME [--postgres-only|--mysql-only]" >&2
            exit 1
            ;;
    esac
done

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

# The alembic revision that is the *parent* of migration 051 (09e0cfebad78).
# Snapshots must be stamped at this revision so that 051 appears unapplied.
PARENT_REVISION="29b539ebc72a"

SNAPSHOT_NAME="snapshot_1_13_4_re_add_run_tags_run_id_idx"

PG_COMPAT_DIR="python_modules/libraries/dagster-postgres/dagster_postgres_tests/compat_tests"
PG_SNAPSHOT_DIR="${PG_COMPAT_DIR}/${SNAPSHOT_NAME}/postgres"
PG_DUMP_FILE="${PG_SNAPSHOT_DIR}/pg_dump.txt"
PG_COMPOSE="python_modules/libraries/dagster-postgres/dagster_postgres/test_fixtures/docker-compose.yml"

MYSQL_COMPAT_DIR="python_modules/libraries/dagster-mysql/dagster_mysql_tests/compat_tests"
MYSQL_DUMP_FILE="${MYSQL_COMPAT_DIR}/${SNAPSHOT_NAME}.sql"
MYSQL_COMPOSE="python_modules/libraries/dagster-mysql/dagster_mysql_tests/docker-compose.yml"

PG_USER="test"
PG_PASSWORD="test"
PG_DB="test"

MYSQL_USER="root"
MYSQL_PASSWORD="test"
MYSQL_DB="test"

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

log() { echo "[$SCRIPT_NAME] $*"; }
die() { echo "[$SCRIPT_NAME] ERROR: $*" >&2; exit 1; }

require_cmd() {
    command -v "$1" &>/dev/null || die "'$1' not found. Install it or set up the venv: uv venv --python 3.12 && source .venv/bin/activate && make dev_install"
}

# Run psql inside the already-running postgres container.
pg_exec() {
    docker compose -f "$PG_COMPOSE" exec -T \
        -e PGPASSWORD="$PG_PASSWORD" \
        postgres \
        psql -h localhost -U "$PG_USER" "$@"
}

# Dump from inside the postgres container, stream to stdout on the host.
pg_dump_to_file() {
    local dest="$1"
    docker compose -f "$PG_COMPOSE" exec -T \
        -e PGPASSWORD="$PG_PASSWORD" \
        postgres \
        pg_dump -h localhost -U "$PG_USER" "$PG_DB" \
        > "$dest"
}

# Run mysql inside the already-running mysql container.
mysql_exec() {
    docker compose -f "$MYSQL_COMPOSE" exec -T \
        -e MYSQL_PWD="$MYSQL_PASSWORD" \
        test-mysql-db \
        mysql -u "$MYSQL_USER" --ssl-mode=DISABLED "$@"
}

# Dump from inside the mysql container, stream to stdout on the host.
mysqldump_to_file() {
    local dest="$1"
    docker compose -f "$MYSQL_COMPOSE" exec -T \
        -e MYSQL_PWD="$MYSQL_PASSWORD" \
        test-mysql-db \
        mysqldump -u "$MYSQL_USER" --ssl-mode=DISABLED "$MYSQL_DB" \
        > "$dest"
}

wait_for_postgres() {
    log "Waiting for PostgreSQL to be ready..."
    local attempts=0
    until pg_exec -d "$PG_DB" -c '\q' &>/dev/null; do
        attempts=$((attempts + 1))
        [ "$attempts" -ge 30 ] && die "PostgreSQL did not become ready within 15s"
        sleep 0.5
    done
    log "PostgreSQL is ready."
}

wait_for_mysql() {
    log "Waiting for MySQL to be ready..."
    local attempts=0
    until mysql_exec -e "SELECT 1" &>/dev/null; do
        attempts=$((attempts + 1))
        if [ "$attempts" -ge 30 ]; then
            log "MySQL did not become ready within 15s. Last error output:"
            mysql_exec -e "SELECT 1" || true
            die "MySQL did not become ready within 15s"
        fi
        sleep 0.5
    done
    log "MySQL is ready."
}

# Write a temporary dagster.yaml into TMPDIR and echo the path.
# $1 = db type: "postgres" or "mysql"
write_dagster_yaml() {
    local db_type="$1"
    local yaml_dir
    yaml_dir="$(mktemp -d)"

    if [ "$db_type" = "postgres" ]; then
        # dagster instance migrate runs on the host; connect via the published port.
        cat > "$yaml_dir/dagster.yaml" <<YAML
run_storage:
  module: dagster_postgres.run_storage
  class: PostgresRunStorage
  config:
    postgres_url: "postgresql://${PG_USER}:${PG_PASSWORD}@127.0.0.1:5432/${PG_DB}"

event_log_storage:
  module: dagster_postgres.event_log
  class: PostgresEventLogStorage
  config:
    postgres_url: "postgresql://${PG_USER}:${PG_PASSWORD}@127.0.0.1:5432/${PG_DB}"

schedule_storage:
  module: dagster_postgres.schedule_storage
  class: PostgresScheduleStorage
  config:
    postgres_url: "postgresql://${PG_USER}:${PG_PASSWORD}@127.0.0.1:5432/${PG_DB}"
YAML
    elif [ "$db_type" = "mysql" ]; then
        # dagster instance migrate runs on the host; connect via the published port.
        cat > "$yaml_dir/dagster.yaml" <<YAML
run_storage:
  module: dagster_mysql.run_storage
  class: MySQLRunStorage
  config:
    mysql_url: "mysql+mysqlconnector://${MYSQL_USER}:${MYSQL_PASSWORD}@127.0.0.1:3306/${MYSQL_DB}"

event_log_storage:
  module: dagster_mysql.event_log
  class: MySQLEventLogStorage
  config:
    mysql_url: "mysql+mysqlconnector://${MYSQL_USER}:${MYSQL_PASSWORD}@127.0.0.1:3306/${MYSQL_DB}"

schedule_storage:
  module: dagster_mysql.schedule_storage
  class: MySQLScheduleStorage
  config:
    mysql_url: "mysql+mysqlconnector://${MYSQL_USER}:${MYSQL_PASSWORD}@127.0.0.1:3306/${MYSQL_DB}"
YAML
    else
        die "Unknown db_type: $db_type"
    fi

    echo "$yaml_dir"
}

# ──────────────────────────────────────────────────────────────────────────────
# PostgreSQL snapshot
# ──────────────────────────────────────────────────────────────────────────────

create_postgres_snapshot() {
    log "=== Creating PostgreSQL snapshot ==="

    require_cmd dagster

    log "Starting PostgreSQL via docker compose..."
    docker compose -f "$PG_COMPOSE" up -d
    wait_for_postgres

    # Drop and recreate the database to start from a clean state.
    log "Wiping database ${PG_DB}..."
    pg_exec -d postgres \
        -c "DROP DATABASE IF EXISTS ${PG_DB};" \
        -c "CREATE DATABASE ${PG_DB} OWNER ${PG_USER};"

    # Run `dagster instance migrate` to create schema at HEAD.
    local yaml_dir
    yaml_dir="$(write_dagster_yaml postgres)"
    log "Running 'dagster instance migrate' to bring schema to HEAD..."
    DAGSTER_HOME="$yaml_dir" dagster instance migrate
    DAGSTER_HOME="$yaml_dir" dagster instance reindex

    # Dump the result.
    log "Dumping PostgreSQL to ${PG_DUMP_FILE}..."
    mkdir -p "$PG_SNAPSHOT_DIR"
    pg_dump_to_file "$PG_DUMP_FILE"

    log "Stopping PostgreSQL docker compose..."
    docker compose -f "$PG_COMPOSE" down

    rm -rf "$yaml_dir"
    log "PostgreSQL snapshot written to ${PG_DUMP_FILE}"
}

# ──────────────────────────────────────────────────────────────────────────────
# MySQL snapshot
# ──────────────────────────────────────────────────────────────────────────────

create_mysql_snapshot() {
    log "=== Creating MySQL snapshot ==="

    require_cmd dagster

    # Use the standard (non-pinned, non-backcompat) MySQL instance on port 3306.
    log "Starting MySQL via docker compose (service: test-mysql-db)..."
    docker compose -f "$MYSQL_COMPOSE" up -d test-mysql-db
    wait_for_mysql

    # Drop and recreate the schema to start from a clean state.
    log "Wiping MySQL database ${MYSQL_DB}..."
    mysql_exec -e "DROP DATABASE IF EXISTS ${MYSQL_DB}; CREATE DATABASE ${MYSQL_DB};"

    # Run `dagster instance migrate` to create schema at HEAD.
    local yaml_dir
    yaml_dir="$(write_dagster_yaml mysql)"
    log "Running 'dagster instance migrate' to bring schema to HEAD..."
    DAGSTER_HOME="$yaml_dir" dagster instance migrate
    DAGSTER_HOME="$yaml_dir" dagster instance reindex

    # Dump the result.
    log "Dumping MySQL to ${MYSQL_DUMP_FILE}..."
    mysqldump_to_file "$MYSQL_DUMP_FILE"

    log "Stopping MySQL docker compose (service: test-mysql-db)..."
    docker compose -f "$MYSQL_COMPOSE" down test-mysql-db

    rm -rf "$yaml_dir"
    log "MySQL snapshot written to ${MYSQL_DUMP_FILE}"
}

# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

log "Snapshot name: ${SNAPSHOT_NAME}"
log "Parent alembic revision: ${PARENT_REVISION}"

[ "$DO_POSTGRES" -eq 1 ] && create_postgres_snapshot
[ "$DO_MYSQL" -eq 1 ]    && create_mysql_snapshot

log "Done. Snapshot files:"
[ "$DO_POSTGRES" -eq 1 ] && echo "  Postgres: ${PG_DUMP_FILE}"
[ "$DO_MYSQL" -eq 1 ]    && echo "  MySQL:    ${MYSQL_DUMP_FILE}"

log ""
log "Next steps:"
log "  1. Switch back to your working branch:  git checkout -"
log "  2. Re-activate the venv for that branch: source .venv/bin/activate"
log "  3. Review the dump files to confirm:"
log "       - alembic_version = '${PARENT_REVISION}'"
log "       - idx_run_tags(key, value) IS present"
log "       - idx_run_tags_run_id IS NOT present"
log "  4. Run the backcompat tests:"
log "       pytest python_modules/libraries/dagster-postgres/dagster_postgres_tests/compat_tests/test_back_compat.py::test_re_add_run_tags_run_id_idx"
log "       pytest python_modules/libraries/dagster-mysql/dagster_mysql_tests/compat_tests/test_back_compat.py::test_re_add_run_tags_run_id_idx"
