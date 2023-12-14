import concurrent.futures
import os
import re
from urllib.parse import parse_qs

import database as db
import pandas as pd
from dotenv import load_dotenv
from loguru import logger
from scraper import get_page_content
from sqlalchemy import func

date_regexp = re.compile(r"(\d{2}-\d{2}-\d{4})")


def get_voting_url_details(query_string: str) -> tuple[int, int, int]:
    corrected_query_string = query_string.replace("&amp;", "&")
    parsed_query = parse_qs(corrected_query_string)
    term_number = int(parsed_query["NrKadencji"][0])
    sitting_number = int(parsed_query["NrPosiedzenia"][0])
    voting_number = int(parsed_query["NrGlosowania"][0])
    return (term_number, sitting_number, voting_number)


def get_day_id(query_string: str) -> int:
    parsed_query = parse_qs(query_string)
    return int(parsed_query["IdDnia"][0])


def last_saved_sitting() -> int:
    with db.Session.begin() as session:
        result = session.query(func.max(db.Votings.SittingDayId)).one()
    last_saved_day_id = result[0] or 0
    return last_saved_day_id


def get_last_available_day() -> int:
    last_term_menu_soup = get_page_content(
        "https://www.sejm.gov.pl/Sejm10.nsf/terminarz.xsp"
    )
    last_term_rel_link = last_term_menu_soup.find(
        "a", string="Głosowania na posiedzeniach Sejmu"
    )
    last_term_rel_link = last_term_rel_link["href"]
    last_term_abs_link = "https://www.sejm.gov.pl/" + last_term_rel_link

    last_term_sittings_soup = get_page_content(last_term_abs_link)
    sittings_table = last_term_sittings_soup.select_one("table")
    table_rows = sittings_table.find_all("tr")
    last_voting_row = table_rows[-1].find_all("td")
    last_voting_url = last_voting_row[1].select_one("a")["href"]
    return get_day_id(last_voting_url)


def scrape_voting(url: str) -> None:
    try:
        logger.debug(f"Scraping {url}")
        soup = get_page_content(url)

        table_title = soup.select_one("h1")
        table_title = table_title.text
        voting_table = soup.select_one("table")
        table_rows = voting_table.find_all("tr")

        data = []
        for row in table_rows[1:]:
            cols = row.find_all("td")

            sitting_day_id = get_day_id(url)
            voting_date = date_regexp.findall(table_title)[0]
            voting_url = cols[0].select_one("a")["href"]

            term_number, sitting_number, voting_number = get_voting_url_details(
                voting_url
            )
            full_voting_url = "https://www.sejm.gov.pl/Sejm10.nsf/" + voting_url
            hour = cols[1].text
            topic = cols[2].text

            data.append(
                {
                    "SittingDayId": sitting_day_id,
                    "Date": voting_date,
                    "TermNumber": term_number,
                    "SittingNumber": sitting_number,
                    "VotingNumber": voting_number,
                    "SittingUrl": url,
                    "VotingUrl": full_voting_url,
                    "SittingTitle": table_title,
                    "Time": hour,
                    "VotingTopic": topic,
                }
            )

        df = pd.DataFrame(data)
        df["SittingDayId"] = df["SittingDayId"].astype(int)

        df["VotingTimestamp"] = df["Date"] + " " + df["Time"]
        df["VotingTimestamp"] = pd.to_datetime(
            df["VotingTimestamp"], format="%d-%m-%Y %H:%M:%S"
        )
        df = df.drop(columns=["Date", "Time"])

        df["TermNumber"] = df["TermNumber"].astype(int)
        df["SittingNumber"] = df["SittingNumber"].astype(int)
        df["VotingNumber"] = df["VotingNumber"].astype(int)
        df["SittingUrl"] = df["SittingUrl"].astype(str)
        df["VotingUrl"] = df["VotingUrl"].astype(str)
        df["SittingTitle"] = df["SittingTitle"].astype(str)
        df["VotingTopic"] = df["VotingTopic"].astype(str)

        df.to_sql("Votings", db.engine, if_exists="append", index=False)
        logger.debug(f"Finished scraping {url}")

    except Exception as e:
        logger.error(f"An exception occurred with URL {url}: {e}")


if __name__ == "__main__":
    load_dotenv()

    MAX_THREADS = int(os.environ["SCRAPINGBEE_CONCURRENT"])

    LAST_SAVED_SITTING = last_saved_sitting()
    LAST_AVAILABLE_SITTING = get_last_available_day()

    urls = [
        f"https://www.sejm.gov.pl/Sejm10.nsf/agent.xsp?symbol=listaglos&IdDnia={day}"
        for day in range(LAST_SAVED_SITTING + 1, LAST_AVAILABLE_SITTING + 1)
    ]

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        results = executor.map(scrape_voting, urls)
