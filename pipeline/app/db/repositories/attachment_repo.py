from sqlalchemy import insert, text
#
from app.db.engine import get_engine
from app.db.tables import bid_notice_attachment
#
#
async def bulk_insert_attachments(rows: list[dict]):
    if not rows:
        return

    engine = get_engine()
    async with engine.begin() as conn:
        await conn.execute(
            insert(bid_notice_attachment),
            rows
        )

CLAIM_SQL = text("""
WITH cte AS (
    SELECT id
    FROM bid_notice_attachment
    WHERE status IN ('pending', 'failed')
    AND (
        status = 'pending'
        OR (
            status = 'failed'
            AND updated_at < now() - interval '5 minutes'
        )
    )
    AND attempts < :max_attempts
    ORDER BY
        CASE status WHEN 'pending' THEN 0 ELSE 1 END,
        created_at
    LIMIT :limit
    FOR UPDATE SKIP LOCKED
)
UPDATE bid_notice_attachment a
SET status = 'in_progress',
    attempts = attempts + 1,
    started_at = COALESCE(started_at, now()),
    updated_at = now()
FROM cte
WHERE a.id = cte.id
RETURNING a.*;
""")

async def claim_attachments(
    limit: int = 10,
    max_attempts: int = 3,
) -> list[dict]:
    engine = get_engine()
    async with engine.begin() as conn:
        res = await conn.execute(
            CLAIM_SQL,
            {"limit": limit, "max_attempts": max_attempts},
        )
        return res.mappings().all()

RECOVER_STUCK_SQL = text("""
UPDATE bid_notice_attachment
SET status = 'pending',
    updated_at = now()
WHERE status = 'in_progress'
  AND started_at < now() - interval '30 minutes';
""")

async def recover_stuck_attachments():
    engine = get_engine()
    async with engine.begin() as conn:
        result = await conn.execute(RECOVER_STUCK_SQL)
        return result.rowcount

MARK_DOWNLOADED_SQL = text("""
UPDATE bid_notice_attachment
SET status = 'downloaded',
    storage_path = :storage_path,
    file_size = :file_size,
    file_hash = :file_hash,
    finished_at = now(),
    updated_at = now(),
    last_error = NULL
WHERE id = :id
  AND status = 'in_progress';
""")

MARK_FAILED_SQL = text("""
UPDATE bid_notice_attachment
SET status = 'failed',
    last_error = :error,
    finished_at = now(),
    updated_at = now()
WHERE id = :id
  AND status = 'in_progress';
""")

async def mark_downloaded(
    id_: int,
    storage_path: str,
    file_size: int,
    file_hash: str,
):
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.execute(
            MARK_DOWNLOADED_SQL,
            {
                "id": id_,
                "storage_path": storage_path,
                "file_size": file_size,
                "file_hash": file_hash,
            },
        )

async def mark_failed(id_: int, error: str):
    engine = get_engine()
    async with engine.begin() as conn:
        await conn.execute(
            MARK_FAILED_SQL,
            {"id": id_, "error": error[:2000]},
        )


