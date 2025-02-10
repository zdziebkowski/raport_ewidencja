import logging
from pathlib import Path
from datetime import datetime


def setup_logger(name: str) -> logging.Logger:
    """Sets up a logger with a timestamped log file in `logs/`."""

    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)  # Ensure logs directory exists
    log_filename = log_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_logs_{name}.log"

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
