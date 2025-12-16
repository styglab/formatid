import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
#
from app.core.deps import get_config


settings = get_config()

BASE_DIR = Path(__file__).resolve().parents[2]
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "app.log"

# 로거 설정
logger = logging.getLogger(settings.APP_NAME) # pipeline
logger.setLevel(logging.INFO)  # DEBUG / INFO / WARNING / ERROR / CRITICAL

formatter = logging.Formatter(
    "[%(asctime)s] [%(levelname)s] [%(name)s] "
    "%(filename)s:%(lineno)d | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# 콘솔 출력 핸들러
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)

# 파일 핸들러 (하루 단위 롤링)
file_handler = TimedRotatingFileHandler(
    LOG_FILE,
    when="midnight",  # 자정마다 새 파일
    interval=1,
    backupCount=30,   # 30일 로그 보관
    encoding="utf-8",
)
file_handler.setFormatter(formatter)

# 로거에 핸들러 등록
if not logger.handlers:
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

def get_logger(name: str = None):
    """모듈 단위 로거 생성 (같은 설정 공유)"""
    return logger.getChild(name) if name else logger

