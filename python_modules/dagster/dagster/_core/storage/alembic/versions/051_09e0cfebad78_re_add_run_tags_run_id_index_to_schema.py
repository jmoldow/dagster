"""re-add run_tags_run_id index to schema

Revision ID: 09e0cfebad78
Revises: 29b539ebc72a
Create Date: 2026-05-07 13:47:40.058063

This addresses two problems that existed in 047_add_run_tags_run_id_idx (revision=6b7fb194ff9c):

- Index not in schema.py: idx_run_tags_run_id was never added to storage/runs/schema.py, and
  idx_run_tags was never removed. So fresh installs of Dagster still get the old, less-useful index.

- Index could be invalid and unusable (probably postgresql-specific): On postgresql, the index is
  created using `CREATE UNIQUE INDEX CONCURRENTLY`. This can fail if for some reason there are
  duplicate (key, value, run_id, id) tuples (in my case, there were about 200 duplicates, mostly for
  key='dagster/will_retry'). Possibly because of the CONCURRENTLY, the index is left in a
  partially-created state. It shows up in
  `SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'run_tags'` queries. But
  `SELECT indexrelid::regclass, indisvalid, indisready FROM pg_index WHERE indrelid = 'run_tags'::regclass`
  reveals that indisvalid and indisready are both false. When the duplicate key is detected, an
  exception is thrown and the migration exits. But because the index technically exists,
  `if not has_index("run_tags", "idx_run_tags_run_id")` is False on the next run, and so the index
  creation isn't re-tried.

This migration duplicates 047_add_run_tags_run_id_idx, but with some improvements/changes:
- If the postgres index might be invalid, start by dropping it so that it can be re-created.
- If `op.create_index("idx_run_tags_run_id")` throws any exception, run
  `op.drop_index("idx_run_tags_run_id")` so that the index isn't left in an invalid state.
- Since idx_run_tags_run_id is supposed to have already existed, and idx_run_tags is supposed to
  have already been dropped, do not provide a downgrade migration.
"""

from alembic import op
from dagster._core.storage.migration.utils import has_index, has_table
from sqlalchemy import inspect, text

# revision identifiers, used by Alembic.
revision = "09e0cfebad78"
down_revision = "29b539ebc72a"
branch_labels = None
depends_on = None


def _is_postgresql_index_valid() -> bool:
    """True if postgres believes the index is valid+ready+live and a unique key. False otherwise.

    Should be called after ensuring that `"postgresql" in inspector.dialect.dialect_description`.
    """
    conn = op.get_bind()
    try:
        result = conn.execute(
            text(
                "SELECT indexrelid::regclass, indisvalid, indisready, indislive, indisunique FROM pg_index "
                "JOIN pg_class ON pg_class.oid = pg_index.indexrelid "
                "WHERE pg_class.relname = 'idx_run_tags_run_id' "
                "AND pg_index.indrelid = 'run_tags'::regclass "
                "AND pg_index.indisvalid AND pg_index.indisready "
                "AND pg_index.indislive AND pg_index.indisunique "
            )
        )
        row = result.fetchone()
        return row is not None
    except Exception:
        # If the query is incorrect, or fails with a transient error, assume the index is invalid.
        return False


def upgrade() -> None:
    inspector = inspect(op.get_bind())

    if has_table("run_tags"):
        if "postgresql" in inspector.dialect.dialect_description:
            if has_index("run_tags", "idx_run_tags_run_id"):
                if not _is_postgresql_index_valid():
                    # Drop the possibly-invalid index so that it can be re-created.
                    op.drop_index(
                        "idx_run_tags_run_id",
                        "run_tags",
                        postgresql_concurrently=True,
                    )
        if not has_index("run_tags", "idx_run_tags_run_id"):
            try:
                op.create_index(
                    "idx_run_tags_run_id",
                    "run_tags",
                    ["key", "value", "run_id"],
                    unique=True,
                    postgresql_concurrently=True,
                    mysql_length={"key": 64, "value": 64, "run_id": 255},
                )
            except Exception:
                # If `op.create_index` throws any exception, drop the index so that it isn't left in
                # an invalid state.
                if has_index("run_tags", "idx_run_tags_run_id"):
                    op.drop_index(
                        "idx_run_tags_run_id",
                        "run_tags",
                        postgresql_concurrently=True,
                    )
                raise
        if has_index("run_tags", "idx_run_tags"):
            op.drop_index(
                "idx_run_tags",
                "run_tags",
                postgresql_concurrently=True,
            )


def downgrade() -> None:
    pass
