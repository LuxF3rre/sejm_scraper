import os
import sys

from loguru import logger

IS_DEBUG = os.getenv("SEJM_SCRAPER_DEBUG", None) == "true"

logger.configure(
    handlers=[
        {
            "sink": sys.stdout,
            "level": "DEBUG" if IS_DEBUG else "INFO",
        }
    ]
)
