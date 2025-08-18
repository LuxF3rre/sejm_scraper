import os
import sys

from loguru import logger

DEBUG = os.getenv("SEJM_SCRAPER_DEBUG", None) == "true"

logger.configure(
    handlers=[
        {
            "sink": sys.stdout,
            "level": "DEBUG" if DEBUG else "INFO",
        }
    ]
)
