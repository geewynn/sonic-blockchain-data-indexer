import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from utils.config import LOG_DIR, LOG_LEVEL, LOG_FILE, LOG_MAX_BYTES, LOG_BACKUPS


def get_logger(name: str = "logs") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(LOG_LEVEL)
    Path(LOG_DIR).mkdir(exist_ok=True)
    logfile = Path(LOG_DIR) / LOG_FILE

    file_handler = RotatingFileHandler(
        logfile,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUPS,
    )
    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    # console handler
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    return logger
