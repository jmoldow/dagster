#!/usr/bin/env bash
# fix_unique_index.sh
#
# Automates the duplicate-row audit, validation, and deletion steps from the
# "could not create unique index / UniqueViolation" section of
# dagster-db-migration-runbook.md (phases 2–4 of the remediation path).
#
# Usage:
#   export EQ_DAGSTER_EQ_CONTEXT=eqdev01
#   projects/eq_dagster/infra/fix_unique_index.sh [options]
#
# Options:
#   --table       TABLE       Table name (default: run_tags)
#   --index       INDEX       Index name to verify (default: idx_run_tags_run_id)
#   --unique-cols COL,COL,... Comma-separated unique constraint columns
#                             (default: key,value,run_id)
#   --dry-run                 Skip destructive SQL; print it instead
#   --help                    Show this help text

set -eu -o pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
TABLE="run_tags"
INDEX="idx_run_tags_run_id"
UNIQUE_COLS="key,value,run_id"
DRY_RUN=0

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --table)       TABLE="$2";       shift 2 ;;
        --index)       INDEX="$2";       shift 2 ;;
        --unique-cols) UNIQUE_COLS="$2"; shift 2 ;;
        --dry-run)     DRY_RUN=1;        shift   ;;
        --help|-h)
            awk '/^# fix_unique_index/{found=1} found && /^set /{exit} found{sub(/^# ?/,""); print}' "$0"
            exit 0
            ;;
        *)
            echo "ERROR: Unknown argument: $1" >&2
            echo "Run with --help for usage." >&2
            exit 1
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Validate required environment
# ---------------------------------------------------------------------------
: "${EQ_DAGSTER_EQ_CONTEXT:?EQ_DAGSTER_EQ_CONTEXT must be set (e.g. eqdev01)}"

# ---------------------------------------------------------------------------
# Derived configuration
# ---------------------------------------------------------------------------
_dagster_infra_dir() {
    echo "$(git rev-parse --show-toplevel)/projects/eq_dagster/infra"
}

LOGFILE="$(_dagster_infra_dir)/dagster-migrate-log-${EQ_DAGSTER_EQ_CONTEXT}-$(date "+%Y-%m-%d").txt"

# Look up DB host from the gitops values file
GITOPS_VALUES="$HOME/eq-gitops/3p_infra/eq-dagster/src/values-${EQ_DAGSTER_EQ_CONTEXT}.yaml"
if [[ ! -f "$GITOPS_VALUES" ]]; then
    echo "ERROR: Values file not found: $GITOPS_VALUES" >&2
    echo "       Check that ~/eq-gitops is checked out and EQ_DAGSTER_EQ_CONTEXT is correct." >&2
    exit 1
fi
DB_HOST="$(grep 'postgresqlHost' "$GITOPS_VALUES" | head -n1 | awk '{print $2}' | tr -d '"' | tr -d "'")"
if [[ -z "$DB_HOST" ]]; then
    echo "ERROR: Could not find postgresqlHost in $GITOPS_VALUES" >&2
    exit 1
fi

DB_USER="postgres"
DB_NAME="postgres"
DB_PORT="5432"
SSL_CERT="global-bundle.pem"
SSL_DOWNLOAD_URL="https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"

# Build the unique column list for SQL (comma-separated, each bare identifier)
# and for the self-join ON clause.
IFS=',' read -ra _UCOLS <<< "$UNIQUE_COLS"
# e.g. "key, value, run_id"
UCOLS_SQL="$(printf '%s, ' "${_UCOLS[@]}" | sed 's/, $//')"
# e.g. "t.key, t.value, t.run_id" — used in self-join SELECT lists to avoid ambiguous column references
UCOLS_T_SQL="$(printf 't.%s, ' "${_UCOLS[@]}" | sed 's/, $//')"

# Build the JOIN ON clause: t.col = t2.col AND ...
JOIN_ON=""
for col in "${_UCOLS[@]}"; do
    col="$(echo "$col" | xargs)"  # trim whitespace
    if [[ -n "$JOIN_ON" ]]; then JOIN_ON+=" AND "; fi
    JOIN_ON+="t.${col} = t2.${col}"
done

# Determine whether we are using all defaults (for Phase 5 eligibility)
USING_DEFAULTS=0
if [[ "$TABLE" == "run_tags" && "$INDEX" == "idx_run_tags_run_id" && "$UNIQUE_COLS" == "key,value,run_id" ]]; then
    USING_DEFAULTS=1
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log() {
    # Print to stdout and append to the log file (no secrets in args).
    printf '%s\n' "$*" | tee -a "$LOGFILE"
}

log_section() {
    log ""
    log "============================================================"
    log "$*"
    log "============================================================"
}

confirm() {
    # Prompt for explicit "yes" confirmation on /dev/tty.
    local prompt="${1:-Proceed?}"
    local answer
    printf '\n%s [yes/N] ' "$prompt" > /dev/tty
    read -r answer < /dev/tty
    if [[ "$answer" != "yes" ]]; then
        log "Aborted by user."
        exit 1
    fi
}

run_psql() {
    # Run a SQL string via psql. PGPASSWORD must be set before calling.
    # All output is tee-d to the log file.
    local sql="$1"
    PGPASSWORD="$PGPASSWORD" psql \
        "host=${DB_HOST} port=${DB_PORT} user=${DB_USER} dbname=${DB_NAME} sslmode=verify-full sslrootcert=${SSL_CERT}" \
        --no-password \
        -c "$sql" \
    2>&1 | tee -a "$LOGFILE"
}

run_psql_quiet() {
    # Like run_psql but captures output (does not tee) — for parsing row counts etc.
    local sql="$1"
    PGPASSWORD="$PGPASSWORD" psql \
        "host=${DB_HOST} port=${DB_PORT} user=${DB_USER} dbname=${DB_NAME} sslmode=verify-full sslrootcert=${SSL_CERT}" \
        --no-password \
        --tuples-only \
        --no-align \
        -c "$sql" \
    2>&1
}

dry_run_note() {
    if [[ "$DRY_RUN" -eq 1 ]]; then
        log "[DRY-RUN] Would execute:"
        log "$1"
    fi
}

# ---------------------------------------------------------------------------
# Banner
# ---------------------------------------------------------------------------
log_section "fix_unique_index.sh — $(date)"
log "  EQ_DAGSTER_EQ_CONTEXT : $EQ_DAGSTER_EQ_CONTEXT"
log "  DB host               : $DB_HOST"
log "  Table                 : $TABLE"
log "  Index                 : $INDEX"
log "  Unique columns        : $UNIQUE_COLS"
log "  Dry-run               : $DRY_RUN"
log "  Log file              : $LOGFILE"
if [[ "$DRY_RUN" -eq 1 ]]; then
    log ""
    log "  *** DRY-RUN MODE — no destructive SQL will be executed ***"
fi

# ---------------------------------------------------------------------------
# SSL certificate
# ---------------------------------------------------------------------------
log_section "SSL certificate check"
if [[ -f "$SSL_CERT" ]]; then
    log "Found $SSL_CERT in current directory — OK."
else
    log "WARNING: $SSL_CERT not found in current directory."
    log "It is required for sslmode=verify-full connections to RDS."
    log ""
    log "Download URL: $SSL_DOWNLOAD_URL"
    confirm "Download global-bundle.pem from the AWS trust store URL shown above?"
    log ""
    (set -x && curl -o global-bundle.pem "$SSL_DOWNLOAD_URL")
    if [[ ! -f "$SSL_CERT" ]]; then
        log "ERROR: $SSL_CERT still not found. Aborting."
        exit 1
    fi
    log "Found $SSL_CERT — proceeding."
fi

# ---------------------------------------------------------------------------
# Read password (once, securely from TTY)
# ---------------------------------------------------------------------------
log_section "Database password"
printf 'Enter password for %s@%s:%s/%s: ' "$DB_USER" "$DB_HOST" "$DB_PORT" "$DB_NAME" > /dev/tty
read -rs PGPASSWORD < /dev/tty
printf '\n' > /dev/tty
export PGPASSWORD
log "(Password read from TTY — not logged.)"

# ---------------------------------------------------------------------------
# Phase 0 — Connection test
# ---------------------------------------------------------------------------
log_section "Phase 0 — Connection test"
log "Running \\conninfo to verify connectivity..."
run_psql '\conninfo'

confirm "Connection details look correct. Proceed to Phase 1 (audit)?"

# ---------------------------------------------------------------------------
# Phase 1 — Audit: show duplicate groups
# ---------------------------------------------------------------------------
log_section "Phase 1 — Audit: duplicate row groups"

AUDIT_SQL="
SELECT ${UCOLS_SQL}, COUNT(*) AS cnt,
       array_agg(id ORDER BY id) AS ids
FROM ${TABLE}
GROUP BY ${UCOLS_SQL}
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 100;
"

log "Duplicate groups (up to 100 rows shown):"
run_psql "$AUDIT_SQL"

# Count total duplicate rows (sum of extras per group, i.e. cnt-1 per group)
DUP_COUNT_SQL="
SELECT COALESCE(SUM(cnt - 1), 0) AS extra_rows
FROM (
    SELECT COUNT(*) AS cnt
    FROM ${TABLE}
    GROUP BY ${UCOLS_SQL}
    HAVING COUNT(*) > 1
) g;
"
EXTRA_ROWS="$(run_psql_quiet "$DUP_COUNT_SQL" | tr -d ' ')"
log ""
log "Total extra (duplicate) rows to delete: ${EXTRA_ROWS}"

if [[ "$EXTRA_ROWS" == "0" ]]; then
    log ""
    log "No duplicate rows found — skipping Phases 2 and 3."
    log "Proceeding to Phase 4 to check index validity."
else
    confirm "Audit complete. Proceed to Phase 2 (validation spot-checks)?"
fi

if [[ "$EXTRA_ROWS" != "0" ]]; then

# ---------------------------------------------------------------------------
# Phase 2 — Validate: spot-check original (to-keep) ids
# ---------------------------------------------------------------------------
log_section "Phase 2 — Validation: self-join spot-checks"

KEEP_IDS_SQL="
SELECT ids[1]
FROM (
    SELECT array_agg(id ORDER BY id) AS ids
    FROM ${TABLE}
    GROUP BY ${UCOLS_SQL}
    HAVING COUNT(*) > 1
) sub
LIMIT 10;
"

log "Fetching up to 10 sample 'original' ids (the ones that will be kept)..."
SAMPLE_IDS="$(run_psql_quiet "$KEEP_IDS_SQL" | grep -E '^[0-9]+$' || true)"

if [[ -z "$SAMPLE_IDS" ]]; then
    log "WARNING: Could not parse sample IDs for spot-checks. Proceeding anyway."
else
    log ""
    log "NOTE: Each of the following queries is EXPECTED to return multiple rows"
    log "      (the duplicates are still present at this stage)."
    log ""
    while IFS= read -r sample_id; do
        [[ -z "$sample_id" ]] && continue
        VALIDATE_SQL="
SELECT t.id, ${UCOLS_T_SQL}
FROM ${TABLE} t
JOIN ${TABLE} t2
  ON ${JOIN_ON}
WHERE t2.id = ${sample_id};
"
        log "--- Spot-check for original id=${sample_id} ---"
        run_psql "$VALIDATE_SQL"
    done <<< "$SAMPLE_IDS"
fi

confirm "Spot-checks look as expected (multiple rows per group). Proceed to Phase 3 (delete duplicates)?"

# ---------------------------------------------------------------------------
# Phase 3 — Delete duplicates (transaction)
# ---------------------------------------------------------------------------
log_section "Phase 3 — Delete duplicate rows"

DELETE_SQL="
DELETE FROM ${TABLE}
WHERE id IN (
    SELECT unnest(ids[2:])
    FROM (
        SELECT array_agg(id ORDER BY id) AS ids
        FROM ${TABLE}
        GROUP BY ${UCOLS_SQL}
        HAVING COUNT(*) > 1
    ) dupes
);
"

if [[ "$DRY_RUN" -eq 1 ]]; then
    dry_run_note "BEGIN;"
    dry_run_note "$DELETE_SQL"
    log ""
    log "[DRY-RUN] Skipping actual BEGIN/DELETE/COMMIT."
    log "Re-run without --dry-run to apply."
else
    log "Opening transaction and deleting duplicates..."
    run_psql "BEGIN;"

    log "Executing DELETE..."
    run_psql "$DELETE_SQL"

    # Re-run spot-check validation — should now show exactly 1 row per id
    log ""
    log "Post-DELETE validation (each query should now return exactly 1 row):"
    if [[ -n "$SAMPLE_IDS" ]]; then
        while IFS= read -r sample_id; do
            [[ -z "$sample_id" ]] && continue
            VALIDATE_SQL="
SELECT t.id, ${UCOLS_T_SQL}
FROM ${TABLE} t
JOIN ${TABLE} t2
  ON ${JOIN_ON}
WHERE t2.id = ${sample_id};
"
            log "--- Post-delete spot-check for id=${sample_id} ---"
            run_psql "$VALIDATE_SQL"
        done <<< "$SAMPLE_IDS"
    fi

    log ""
    log "Please inspect the DELETE row count and spot-check results above."
    confirm "Results look correct. COMMIT the transaction?"

    run_psql "COMMIT;"
    log "Transaction committed."
fi

fi  # end: EXTRA_ROWS != 0

# ---------------------------------------------------------------------------
# Phase 4 — Verify index presence and validity
# ---------------------------------------------------------------------------
log_section "Phase 4 — Index presence and validity check"

INDEX_VALID_SQL="
SELECT indexrelid::regclass, indisvalid, indisready, indislive
FROM pg_index
JOIN pg_class ON pg_class.oid = pg_index.indexrelid
WHERE pg_class.relname = '${INDEX}'
  AND pg_index.indrelid = '${TABLE}'::regclass;
"

log "Checking presence and validity of index '${INDEX}' on table '${TABLE}':"
run_psql "$INDEX_VALID_SQL"

# Determine index state: "absent", "valid", or "invalid"
# run_psql_quiet with --tuples-only --no-align gives: "indexname|t|t|t" per row, or empty if absent
INDEX_ROW="$(run_psql_quiet "$INDEX_VALID_SQL" | grep -v '^$' | head -n1 || true)"
if [[ -z "$INDEX_ROW" ]]; then
    INDEX_STATE="absent"
    log ""
    log "Index '${INDEX}' does not exist on table '${TABLE}'."
else
    INDISVALID="$(echo "$INDEX_ROW" | cut -d'|' -f2 | tr -d ' ')"
    if [[ "$INDISVALID" == "t" ]]; then
        INDEX_STATE="valid"
        log ""
        log "Index '${INDEX}' is present and valid (indisvalid=t). No rebuild needed."
        log "You can now resume the Alembic migration."
    else
        INDEX_STATE="invalid"
        log ""
        log "Index '${INDEX}' is present but NOT valid (indisvalid=${INDISVALID})."
    fi
fi

if [[ "$INDEX_STATE" != "valid" ]]; then
    if [[ "$USING_DEFAULTS" -eq 1 ]]; then
        log "Proceeding to Phase 5 to (re)create the index."
    else
        log "Phase 5 (automatic rebuild) is only offered for the default index."
        log "Please manually DROP (if present) and recreate '${INDEX}' on '${TABLE}'."
        log "Remember: CREATE UNIQUE INDEX CONCURRENTLY cannot run inside a transaction."
    fi
fi

# ---------------------------------------------------------------------------
# Phase 5 — (Re)create index (only for default table/index/cols)
# ---------------------------------------------------------------------------
if [[ "$USING_DEFAULTS" -eq 1 && "$INDEX_STATE" != "valid" ]]; then
    log_section "Phase 5 — (Re)create index (${INDEX})"

    CREATE_SQL="CREATE UNIQUE INDEX CONCURRENTLY ${INDEX}
    ON public.${TABLE} USING btree (key, value, run_id);"

    if [[ "$DRY_RUN" -eq 1 ]]; then
        if [[ "$INDEX_STATE" == "invalid" ]]; then
            dry_run_note "DROP INDEX ${INDEX};"
        else
            log "[DRY-RUN] Index '${INDEX}' is absent — no DROP needed."
        fi
        dry_run_note "$CREATE_SQL"
        log "[DRY-RUN] Skipping DROP INDEX / CREATE INDEX CONCURRENTLY."
    else
        log ""
        log "Actions to be taken:"
        if [[ "$INDEX_STATE" == "invalid" ]]; then
            log "  DROP INDEX ${INDEX};                    (index is present but invalid)"
        else
            log "  (skipping DROP — index '${INDEX}' is absent)"
        fi
        log "  $CREATE_SQL"
        log ""
        log "NOTE: CREATE INDEX CONCURRENTLY cannot run inside a transaction block."
        log "      DROP and CREATE are executed as separate psql calls."
        confirm "Proceed?"

        if [[ "$INDEX_STATE" == "invalid" ]]; then
            log "Dropping invalid index '${INDEX}'..."
            run_psql "DROP INDEX ${INDEX};"
        fi

        log "Creating unique index concurrently (this may take a while)..."
        # CREATE INDEX CONCURRENTLY must run outside a transaction block;
        # psql autocommit is correct here — do NOT wrap in BEGIN.
        PGPASSWORD="$PGPASSWORD" psql \
            "host=${DB_HOST} port=${DB_PORT} user=${DB_USER} dbname=${DB_NAME} sslmode=verify-full sslrootcert=${SSL_CERT}" \
            --no-password \
            -c "$CREATE_SQL" \
        2>&1 | tee -a "$LOGFILE"

        log ""
        log "Verifying index validity after (re)create..."
        run_psql "$INDEX_VALID_SQL"

        # Assert all three flags are true
        RESULT_LINE="$(run_psql_quiet "$INDEX_VALID_SQL" | grep -v '^$' | head -n1 | tr -d ' ' || true)"
        VALID="$(echo "$RESULT_LINE" | cut -d'|' -f2)"
        READY="$(echo "$RESULT_LINE" | cut -d'|' -f3)"
        LIVE="$(echo "$RESULT_LINE"  | cut -d'|' -f4)"

        if [[ "$VALID" == "t" && "$READY" == "t" && "$LIVE" == "t" ]]; then
            log ""
            log "SUCCESS: Index '${INDEX}' is now valid (indisvalid=t, indisready=t, indislive=t)."
        else
            log ""
            log "WARNING: Index may not be fully valid after rebuild."
            log "         indisvalid=${VALID} indisready=${READY} indislive=${LIVE}"
            log "         Please investigate before resuming the Alembic migration."
        fi
    fi
fi

# ---------------------------------------------------------------------------
# Legacy index cleanup (always runs when using defaults)
# Drop idx_run_tags if present — it is superseded by idx_run_tags_run_id.
# ---------------------------------------------------------------------------
if [[ "$USING_DEFAULTS" -eq 1 ]]; then
    log_section "Legacy index cleanup"
    LEGACY_INDEX="idx_run_tags"
    LEGACY_INDEX_SQL="
SELECT 1 FROM pg_class
JOIN pg_index ON pg_class.oid = pg_index.indexrelid
WHERE pg_class.relname = '${LEGACY_INDEX}'
  AND pg_index.indrelid = '${TABLE}'::regclass;
"
    LEGACY_EXISTS="$(run_psql_quiet "$LEGACY_INDEX_SQL" | grep -c '1' || true)"

    if [[ "$LEGACY_EXISTS" -gt 0 ]]; then
        log "Legacy index '${LEGACY_INDEX}' is present."
        if [[ "$DRY_RUN" -eq 1 ]]; then
            dry_run_note "DROP INDEX ${LEGACY_INDEX};"
            log "[DRY-RUN] Skipping DROP."
        else
            confirm "Drop legacy index '${LEGACY_INDEX}'?"
            log "Dropping legacy index '${LEGACY_INDEX}'..."
            run_psql "DROP INDEX ${LEGACY_INDEX};"
            log "Legacy index dropped."
        fi
    else
        log "Legacy index '${LEGACY_INDEX}' is not present — nothing to do."
    fi
fi

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
log_section "Complete — $(date)"
log ""
log "Next steps:"
log "  1. If duplicates were deleted and the index is valid, resume the Alembic"
log "     migration with: dagster-migrate upgrade"
log "  2. If in dry-run mode, re-run without --dry-run to apply changes."
log ""
log "Log file: $LOGFILE"
