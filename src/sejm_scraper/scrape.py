from dataclasses import dataclass
from typing import Optional

import httpx
from loguru import logger

from sejm_scraper import api_client, api_schemas, database, database_key_utils

PLANNED_SITTING_NUMBER = 0


def scrape_terms(
    client: httpx.Client,
    from_term: Optional[int] = None,
) -> list[database.Term]:
    logger.info(
        f"Scraping terms starting from term {from_term}"
        if from_term
        else "Scraping all terms"
    )
    terms = api_client.fetch_terms(client=client)
    if from_term is not None:
        terms = [term for term in terms if term.number >= from_term]
    result = [
        database.Term(
            id=database_key_utils.generate_term_natural_key(term=term),
            number=term.number,
            from_date=term.from_date,
            to_date=term.to_date,
        )
        for term in terms
    ]
    logger.debug(f"Scraped {len(result)} terms")
    return result


def scrape_sittings(
    client: httpx.Client,
    term: database.Term,
    from_sitting: Optional[int] = None,
) -> list[database.Sitting]:
    logger.info(
        f"Scraping sittings for term {term.number}"
        + (f" starting from sitting {from_sitting}" if from_sitting else "")
    )
    sittings = api_client.fetch_sittings(client=client, term=term.number)
    sittings = [
        sitting
        for sitting in sittings
        if sitting.number != PLANNED_SITTING_NUMBER
    ]
    if from_sitting is not None:
        sittings = [
            sitting for sitting in sittings if sitting.number >= from_sitting
        ]
    result = [
        database.Sitting(
            id=database_key_utils.generate_sitting_natural_key(
                sitting=sitting, term=term
            ),
            term_id=term.id,
            title=sitting.title,
            number=sitting.number,
        )
        for sitting in sittings
    ]
    logger.debug(f"Scraped {len(result)} sittings for term {term.number}")
    return result


@dataclass
class ScrapedVotingsResult:
    votings: list[database.Voting]
    voting_options: list[database.VotingOption]


def scrape_votings(
    client: httpx.Client,
    term: database.Term,
    sitting: database.Sitting,
    from_voting: Optional[int] = None,
) -> ScrapedVotingsResult:
    logger.info(
        f"Scraping votings for term {term.number}, sitting {sitting.number}"
        + (f" starting from voting {from_voting}" if from_voting else "")
    )
    votings = api_client.fetch_votings(
        client=client, term=term.number, sitting=sitting.number
    )
    if from_voting is not None:
        votings = [voting for voting in votings if voting.number >= from_voting]

    scraped_votings = []
    scraped_voting_options = []

    for voting in votings:
        voting_id = database_key_utils.generate_voting_natural_key(
            voting=voting, term=term, sitting=sitting
        )
        scraped_votings.append(
            database.Voting(
                id=voting_id,
                sitting_id=sitting.id,
                day_number=voting.day_number,
                number=voting.number,
                date=voting.date,
                title=voting.title,
                description=voting.description,
            )
        )
        if voting.voting_options is not None:
            for voting_option in voting.voting_options:  # ty: ignore
                scraped_voting_options.append(
                    database.VotingOption(
                        id=database_key_utils.generate_voting_option_natural_key(
                            term=term,
                            sitting=sitting,
                            voting=voting,
                            voting_option_index=voting_option.index,
                        ),
                        voting_id=voting_id,
                        index=voting_option.index,
                        description=voting_option.description,
                    )
                )
        else:
            # If there are no voting options, we create a default one
            scraped_voting_options.append(
                database.VotingOption(
                    id=database_key_utils.generate_voting_option_natural_key(
                        term=term,
                        sitting=sitting,
                        voting=voting,
                        voting_option_index=api_schemas.OptionIndex(1),
                    ),
                    voting_id=voting_id,
                    index=api_schemas.OptionIndex(1),
                    description="Default option (no options provided)",
                )
            )

    logger.debug(
        f"Scraped {len(scraped_votings)}"
        f" votings and {len(scraped_voting_options)}"
        f" voting options for term {term.number}, sitting {sitting.number}"
    )
    return ScrapedVotingsResult(
        votings=scraped_votings,
        voting_options=scraped_voting_options,
    )


@dataclass
class ScrapedMpsInTermResult:
    mps_in_term: list[database.MpInTerm]


def scrape_mps_in_term(
    client: httpx.Client,
    term: database.Term,
) -> ScrapedMpsInTermResult:
    logger.info(f"Scraping MPs for term {term.number}")
    mps_in_term = api_client.fetch_mps_in_term(client=client, term=term.number)

    scraped_mps_in_term = []

    for mp in mps_in_term:
        mp_in_term_id = database_key_utils.generate_mp_in_term_natural_key(
            term=term,
            mp=mp,
        )

        scraped_mps_in_term.append(
            database.MpInTerm(
                id=mp_in_term_id,
                in_term_id=mp.in_term_id,
                term_id=term.id,
                first_name=mp.first_name,
                second_name=mp.second_name,
                last_name=mp.last_name,
                birth_date=mp.birth_date,
                birth_place=mp.birth_place,
                education=mp.education,
                profession=mp.profession,
                voivodeship=mp.voivodeship,
                district_name=mp.district_name,
                inactivity_cause=mp.inactivity_cause,
                inactivity_description=mp.inactivity_description,
            )
        )

    logger.debug(
        f"Scraped {len(scraped_mps_in_term)} MPs for term {term.number}"
    )
    return ScrapedMpsInTermResult(
        mps_in_term=scraped_mps_in_term,
    )


def scrape_votes(
    client: httpx.Client,
    term: database.Term,
    sitting: database.Sitting,
    voting: database.Voting,
) -> list[database.Vote]:
    logger.info(
        f"Scraping votes for term {term.number}, sitting {sitting.number},"
        f" voting {voting.number}"
    )
    voting_with_votes = api_client.fetch_votes(
        client=client,
        term=term.number,
        sitting=sitting.number,
        voting=voting.number,
    )

    votes = voting_with_votes.mp_votes

    scraped_votes = []

    for vote in votes:
        mp_in_term_id = database_key_utils.generate_mp_in_term_natural_key(
            term=term,
            mp=api_schemas.MpInTermId(vote.mp_term_id),
        )

        if vote.multiple_option_votes is None:
            if vote.vote == api_schemas.VOTE_VALID:
                msg = (
                    "Invalid vote data: 'multiple_option_votes' "
                    "is None but 'vote' is 'VOTE_VALID'"
                )
                raise ValueError(msg)

            # Single option vote with default vote option
            scraped_votes.append(
                database.Vote(
                    id=database_key_utils.generate_vote_natural_key(
                        term=term,
                        sitting=sitting,
                        voting=voting,
                        voting_option_index=api_schemas.OptionIndex(1),
                        mp_in_term_id=mp_in_term_id,
                    ),
                    voting_option_id=database_key_utils.generate_voting_option_natural_key(
                        term=term,
                        sitting=sitting,
                        voting=voting,
                        voting_option_index=api_schemas.OptionIndex(1),
                    ),
                    mp_in_term_id=mp_in_term_id,
                    vote=vote.vote,
                    party=vote.party,
                )
            )
        else:
            # Multiple options vote
            for voting_option, inner_vote in vote.multiple_option_votes.items():
                scraped_votes.append(
                    database.Vote(
                        id=database_key_utils.generate_vote_natural_key(
                            term=term,
                            sitting=sitting,
                            voting=voting,
                            voting_option_index=voting_option,
                            mp_in_term_id=mp_in_term_id,
                        ),
                        voting_option_id=database_key_utils.generate_voting_option_natural_key(
                            term=term,
                            sitting=sitting,
                            voting=voting,
                            voting_option_index=voting_option,
                        ),
                        mp_in_term_id=mp_in_term_id,
                        vote=inner_vote,
                        party=vote.party,
                    )
                )

    logger.debug(
        f"Scraped {len(scraped_votes)} votes for term {term.number},"
        f" sitting {sitting.number}, voting {voting.number}"
    )
    return scraped_votes
