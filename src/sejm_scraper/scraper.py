import os

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from loguru import logger
from scrapingbee import ScrapingBeeClient

MAX_RETRIES = 5

load_dotenv()


def get_page_content(url: str) -> BeautifulSoup:
    client = ScrapingBeeClient(api_key=os.environ["SCRAPINGBEE_API_KEY"])
    for retry_number in range(MAX_RETRIES):
        response = client.get(
            url,
            params={
                "render_js": "False",
            },
        )
        if response.ok:
            html_content = response.content
            soup = BeautifulSoup(html_content, "html.parser")
            return soup

        logger.warning(f"Failed to GET {url}. Retry {retry_number + 1}/{MAX_RETRIES}")
    logger.error(f"Failed to GET {url}")
    raise RuntimeError


def debug_get_page_content(url: str) -> BeautifulSoup:
    import httpx

    for retry_number in range(MAX_RETRIES):
        response = httpx.get(url=url)
        if response.status_code == 200:
            html_content = response.content
            soup = BeautifulSoup(html_content, "html.parser")
            return soup

        logger.warning(f"Failed to GET {url}. Retry {retry_number + 1}/{MAX_RETRIES}")
    logger.error(f"Failed to GET {url}")
    raise RuntimeError
