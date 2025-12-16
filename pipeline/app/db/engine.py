from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine
#
from app.core.deps import get_config

_engine: AsyncEngine | None = None

def get_engine() -> AsyncEngine:
    global _engine
    
    if _engine is None:
        settings = get_config()

        _engine = create_async_engine(
            settings.DATABASE_URL,
            echo=False,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=1800,
        )
        
    return _engine

