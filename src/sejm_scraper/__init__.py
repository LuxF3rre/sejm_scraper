import os
import sys

from loguru import logger

is_debug = os.getenv("SEJM_SCRAPER_DEBUG", None) == "true"

logger.configure(
    handlers=[
        {
            "sink": sys.stdout,
            "level": "DEBUG" if is_debug else "INFO",
        }
    ]
)
