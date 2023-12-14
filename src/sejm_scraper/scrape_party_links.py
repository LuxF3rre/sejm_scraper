import concurrent.futures
import os
from urllib.parse import parse_qs

import database as db
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from scraper import get_page_content
from sqlalchemy import exists, not_


def get_remaining_voting_identifiers() -> list[tuple[int, int, int]]:
    with db.Session.begin() as session:
        subquery = exists().where(
            (db.PartyVotesLinks.TermNumber == db.Votings.TermNumber)
            & (db.PartyVotesLinks.SittingNumber == db.Votings.SittingNumber)
            & (db.PartyVotesLinks.VotingNumber == db.Votings.VotingNumber)
        )

        query = session.query(
            db.Votings.TermNumber, db.Votings.SittingNumber, db.Votings.VotingNumber
        ).filter(not_(subquery))

        result = [tuple(row) for row in query]
    return result


def get_party_vote_url_details(query_string: str) -> tuple[str, int]:
    corrected_query_string = query_string.replace("&amp;", "&")
    parsed_query = parse_qs(corrected_query_string)
    party = parsed_query["KodKlubu"][0]
    voting_internal_number = int(parsed_query["IdGlosowania"][0])
    return (party, voting_internal_number)


def scrape_party_vote_links(
    term_number: int, sitting_number: int, voting_number: int
) -> None:
    url = f"https://www.sejm.gov.pl/Sejm10.nsf/agent.xsp?symbol=glosowania&NrKadencji={term_number}&NrPosiedzenia={sitting_number}&NrGlosowania={voting_number}"
    logger.debug(f"Scraping {url}")
    soup = get_page_content(url)

    table = soup.select_one("table")
    rows = table.find_all("tr")

    data = []
    for row in rows[1:]:
        cols = row.find_all("td")
        rel_link = cols[0].select_one("a")["href"]
        abs_link = "https://www.sejm.gov.pl/Sejm10.nsf/" + rel_link
        party, voting_internal_id = get_party_vote_url_details(rel_link)

        data.append(
            {
                "Url": abs_link,
                "Party": party,
                "VotingInternalId": voting_internal_id,
                "TermNumber": term_number,
                "SittingNumber": sitting_number,
                "VotingNumber": voting_number,
            }
        )

    df = pd.DataFrame(data)
    df["Url"] = df["Url"].astype(str)
    df["Party"] = df["Party"].astype(str)
    df["VotingInternalId"] = df["VotingInternalId"].astype(int)
    df["TermNumber"] = df["TermNumber"].astype(int)
    df["SittingNumber"] = df["SittingNumber"].astype(int)
    df["VotingNumber"] = df["VotingNumber"].astype(int)

    df.to_sql("PartyVotesLinks", db.engine, if_exists="append", index=False)
    logger.debug(f"Finished scraping {url}")


if __name__ == "__main__":
    load_dotenv()

    MAX_THREADS = int(os.environ["SCRAPINGBEE_CONCURRENT"])

    VOTING_IDENTIFIERS = get_remaining_voting_identifiers()

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        executor.map(lambda x: scrape_party_vote_links(*x), VOTING_IDENTIFIERS)
