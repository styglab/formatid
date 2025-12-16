from app.db.engine import get_engine
from app.db.tables import metadata
from app.db.indexes import (
    CREATE_IDX_ATTACHMENT_STATUS_CREATED,
    CREATE_IDX_ATTACHMENT_NOTICE,
)

async def init_db():
    engine = get_engine()
    
    async with engine.begin() as conn:
        # 1) 테이블 생성
        await conn.run_sync(metadata.create_all)
        # 2) 인덱스 생성
        await conn.execute(CREATE_IDX_ATTACHMENT_STATUS_CREATED)
        await conn.execute(CREATE_IDX_ATTACHMENT_NOTICE)

