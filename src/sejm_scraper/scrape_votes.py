import concurrent.futures
import os

import database as db
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from more_itertools import chunked
from scraper import get_page_content
from sqlalchemy import exists, not_


def get_reminding_vote_identifiers() -> list[tuple[int, str]]:
    with db.Session.begin() as session:
        subquery = exists().where(
            db.Votes.VotingInternalId == db.PartyVotesLinks.VotingInternalId
        )
        query = session.query(
            db.PartyVotesLinks.VotingInternalId, db.PartyVotesLinks.Party
        ).filter(not_(subquery))

        result = [tuple(row) for row in query]
    return result


def scrape_votes(vote_id: int, party: str) -> None:
    url = f"https://www.sejm.gov.pl/Sejm10.nsf/agent.xsp?symbol=klubglos&IdGlosowania={vote_id}&KodKlubu={party}"
    try:
        logger.debug(f"Scraping {url}")
        soup = get_page_content(url)

        table = soup.select_one("table")
        rows = table.find_all("tr")

        data = []
        for row in rows[1:]:
            cols = row.find_all("td")
            for batched in chunked(cols, 3):
                person = batched[1].text
                vote = batched[2].text

                data.append(
                    {
                        "VotingInternalId": vote_id,
                        "Url": url,
                        "Party": party,
                        "Person": person,
                        "Vote": vote,
                    }
                )

        df = pd.DataFrame(data)
        df["VotingInternalId"] = df["VotingInternalId"].astype(int)
        df["Url"] = df["Url"].astype(str)
        df["Party"] = df["Party"].astype(str)
        df["Person"] = df["Person"].astype(str)
        df["Vote"] = df["Vote"].astype(str)

        df.to_sql("Votes", db.engine, if_exists="append", index=False)
        logger.debug(f"Finished scraping {url}")

    except Exception as e:
        logger.error(f"An exception occurred with URL {url}: {e}")


if __name__ == "__main__":
    load_dotenv()

    MAX_THREADS = int(os.environ["SCRAPINGBEE_CONCURRENT"])

    VOTES_IDENTIFIERS = get_reminding_vote_identifiers()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(lambda x: scrape_votes(*x), VOTES_IDENTIFIERS)
