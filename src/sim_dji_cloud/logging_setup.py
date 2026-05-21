import sys
from loguru import logger


def configure_logging(level: str = "INFO", log_file: str | None = None) -> None:
    logger.remove()
    logger.add(sys.stderr, level=level, format="<g>{time:HH:mm:ss.SSS}</g> {level} {message}")
    if log_file:
        logger.add(log_file, level=level, rotation="50 MB", retention=5)
