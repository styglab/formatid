from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import text
#
from app.db.tables import bid_notice
from app.core.db import engine
#
#
async def bulk_upsert_bid_notices(rows: list[dict]) -> int:
    """
    여러 공고를 한 번에 upsert
    return: 처리된 row 수
    """
    if not rows:
        return 0

    stmt = insert(bid_notice).values(rows)

    stmt = stmt.on_conflict_do_update(
        index_elements=["bid_ntce_no", "bid_ntce_ord"],
        set_={
            "raw_json": stmt.excluded.raw_json,
            "updated_at": stmt.excluded.updated_at,
        },
    )

    async with engine.begin() as conn:
        result = await conn.execute(stmt)
        return result.rowcount


SET_LATEST_SQL = text("""
UPDATE bid_notice bn
SET is_latest = TRUE
FROM (
    SELECT
        bid_ntce_no,
        MAX(bid_ntce_ord::int) AS max_ord
    FROM bid_notice
    WHERE bid_ntce_no = ANY(:bid_ntce_nos)
    GROUP BY bid_ntce_no
) t
WHERE bn.bid_ntce_no = t.bid_ntce_no
  AND bn.bid_ntce_ord::int = t.max_ord;
""")

async def set_latest_flags(bid_ntce_nos: list[str]):
    if not bid_ntce_nos:
        return

    async with engine.begin() as conn:
        await conn.execute(
            SET_LATEST_SQL,
            {"bid_ntce_nos": bid_ntce_nos}
        )







#########################################################################
async def upsert_bid_notice(row: dict):
    stmt = insert(bid_notice).values(**row)
    stmt = stmt.on_conflict_do_update(
        index_elements=["bid_ntce_no", "bid_ntce_ord"],
        set_={
            "raw_json": stmt.excluded.raw_json,
            "updated_at": stmt.excluded.updated_at,
            "is_latest": True,
        },
    )

    async with engine.begin() as conn:
        await conn.execute(stmt)


async def bulk_insert_bid_notices(rows: list[dict]):
    async with engine.begin() as conn:
        await conn.execute(insert(bid_notice), rows)




        
