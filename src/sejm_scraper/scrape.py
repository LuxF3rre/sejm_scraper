from dataclasses import dataclass
from itertools import groupby
from operator import attrgetter

import httpx

from sejm_scraper import api_client, api_schemas, database, database_key_utils

PLANNED_SITTING_NUMBER = 0


async def scrape_terms(
    client: httpx.AsyncClient,
    from_term: int | None = None,
) -> list[database.Term]:
    """Scrape Sejm terms from the API and convert to database models.

    Args:
        client: HTTP client instance.
        from_term: If set, only include terms with number >= this value.

    Returns:
        List of Term database models with generated natural keys.
    """
    terms = await api_client.fetch_terms(client=client)
    if from_term is not None:
        terms = [term for term in terms if term.number >= from_term]
    return [
        database.Term(
            id=database_key_utils.generate_term_natural_key(term=term),
            number=term.number,
            from_date=term.from_date,
            to_date=term.to_date,
        )
        for term in terms
    ]


@dataclass
class ScrapedSittingsResult:
    sittings: list[database.Sitting]
    sitting_days: list[database.SittingDay]


async def scrape_sittings(
    client: httpx.AsyncClient,
    term: database.Term,
    from_sitting: int | None = None,
) -> ScrapedSittingsResult:
    """Scrape sittings for a term from the API.

    Filters out planned sittings (number == 0).

    Args:
        client: HTTP client instance.
        term: Term database model to scrape sittings for.
        from_sitting: If set, only include sittings with number >= this value.

    Returns:
        Scraped sittings and their associated sitting days.
    """
    sittings = await api_client.fetch_sittings(client=client, term=term.number)
    sittings = [
        sitting
        for sitting in sittings
        if sitting.number != PLANNED_SITTING_NUMBER
    ]
    if from_sitting is not None:
        sittings = [
            sitting for sitting in sittings if sitting.number >= from_sitting
        ]

    scraped_sittings = []
    scraped_sitting_days = []

    for sitting in sittings:
        sitting_id = database_key_utils.generate_sitting_natural_key(
            sitting=sitting, term=term
        )
        scraped_sittings.append(
            database.Sitting(
                id=sitting_id,
                term_id=term.id,
                title=sitting.title,
                number=sitting.number,
            )
        )
        for day_date in sitting.dates:
            scraped_sitting_days.append(
                database.SittingDay(
                    id=database_key_utils.generate_sitting_day_natural_key(
                        term=term, sitting=sitting, day_date=day_date
                    ),
                    sitting_id=sitting_id,
                    date=day_date,
                )
            )

    return ScrapedSittingsResult(
        sittings=scraped_sittings,
        sitting_days=scraped_sitting_days,
    )


async def discover_sittings_from_votings(
    client: httpx.AsyncClient,
    term: database.Term,
    from_sitting: int | None = None,
) -> ScrapedSittingsResult:
    """Discover sittings from the voting table for terms without proceedings.

    Some older terms (e.g. 3-6) have voting data but no proceedings endpoint.
    The `/term{N}/votings` endpoint returns a flat table mapping dates to
    proceeding (sitting) numbers. This function groups those entries to
    synthesize Sitting and SittingDay records.

    Args:
        client: HTTP client instance.
        term: Term database model.
        from_sitting: If set, only include sittings with number >= this value.

    Returns:
        Scraped sittings and their associated sitting days.
    """
    entries = await api_client.fetch_voting_table(
        client=client, term=term.number
    )
    if from_sitting is not None:
        entries = [e for e in entries if e.proceeding >= from_sitting]

    sorted_entries = sorted(entries, key=attrgetter("proceeding"))

    scraped_sittings = []
    scraped_sitting_days = []

    for proceeding_num, group in groupby(
        sorted_entries, key=attrgetter("proceeding")
    ):
        group_entries = list(group)
        sitting_stub = database.Sitting(
            id="",
            term_id=term.id,
            title=f"Posiedzenie nr {proceeding_num}",
            number=proceeding_num,
        )
        sitting_stub.id = database_key_utils.generate_sitting_natural_key(
            sitting=sitting_stub, term=term
        )
        scraped_sittings.append(sitting_stub)

        unique_dates = sorted({e.sitting_date for e in group_entries})
        for day_date in unique_dates:
            scraped_sitting_days.append(
                database.SittingDay(
                    id=database_key_utils.generate_sitting_day_natural_key(
                        term=term, sitting=sitting_stub, day_date=day_date
                    ),
                    sitting_id=sitting_stub.id,
                    date=day_date,
                )
            )

    return ScrapedSittingsResult(
        sittings=scraped_sittings,
        sitting_days=scraped_sitting_days,
    )


@dataclass
class ScrapedVotingsResult:
    votings: list[database.Voting]
    voting_options: list[database.VotingOption]


async def scrape_votings(
    client: httpx.AsyncClient,
    term: database.Term,
    sitting: database.Sitting,
    from_voting: int | None = None,
) -> ScrapedVotingsResult:
    """Scrape votings and voting options for a sitting.

    Creates default voting options for votings that have none.

    Args:
        client: HTTP client instance.
        term: Term database model.
        sitting: Sitting database model to scrape votings for.
        from_voting: If set, only include votings with number >= this value.

    Returns:
        Scraped votings and their associated voting options.
    """
    votings = await api_client.fetch_votings(
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
                sitting_day=voting.sitting_day,
                number=voting.number,
                date=voting.date.date(),
                title=voting.title,
                description=voting.description,
                topic=voting.topic,
                kind=voting.kind,
                yes=voting.yes,
                no=voting.no,
                abstain=voting.abstain,
                not_participating=voting.not_participating,
                present=voting.present,
                total_voted=voting.total_voted,
                majority_type=voting.majority_type,
                majority_votes=voting.majority_votes,
                against_all=voting.against_all,
            )
        )
        if voting.voting_options is not None:
            for voting_option in voting.voting_options:
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
                        option_label=voting_option.option_label,
                        description=voting_option.description,
                        votes=voting_option.votes,
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
                    option_label="Default option (no options provided)",
                    description=None,
                    votes=0,
                )
            )

    return ScrapedVotingsResult(
        votings=scraped_votings,
        voting_options=scraped_voting_options,
    )


async def scrape_clubs(
    client: httpx.AsyncClient,
    term: database.Term,
) -> list[database.Club]:
    """Scrape clubs for a given term.

    Args:
        client: HTTP client instance.
        term: Term database model to scrape clubs for.

    Returns:
        List of Club database models.
    """
    clubs = await api_client.fetch_clubs(client=client, term=term.number)

    return [
        database.Club(
            id=database_key_utils.generate_club_natural_key(
                club=club, term=term
            ),
            term_id=term.id,
            club_id=club.club_id,
            name=club.name,
            phone=club.phone,
            fax=club.fax,
            email=club.email,
            members_count=club.members_count,
        )
        for club in clubs
    ]


@dataclass
class ScrapedMpsResult:
    mps: list[database.Mp]
    mp_to_term_links: list[database.MpToTermLink]


async def scrape_mps(
    client: httpx.AsyncClient,
    term: database.Term,
) -> ScrapedMpsResult:
    """Scrape MPs and their term links for a given term.

    Args:
        client: HTTP client instance.
        term: Term database model to scrape MPs for.

    Returns:
        Scraped MP records and MP-to-term link records.
    """
    mps = await api_client.fetch_mps(client=client, term=term.number)

    scraped_mps = []
    mp_to_term_links = []

    for mp in mps:
        mp_id = database_key_utils.generate_mp_natural_key(mp=mp)

        scraped_mps.append(
            database.Mp(
                id=mp_id,
                first_name=mp.first_name,
                second_name=mp.second_name,
                last_name=mp.last_name,
                birth_date=mp.birth_date,
                birth_place=mp.birth_place,
            )
        )

        mp_to_term_links.append(
            database.MpToTermLink(
                id=database_key_utils.generate_mp_to_term_link_natural_key(
                    mp=mp, term=term
                ),
                mp_id=mp_id,
                term_id=term.id,
                in_term_id=mp.in_term_id,
                active=mp.active,
                club=mp.club,
                district_num=mp.district_num,
                number_of_votes=mp.number_of_votes,
                email=mp.email,
                education=mp.education,
                profession=mp.profession,
                voivodeship=mp.voivodeship,
                district_name=mp.district_name,
                inactivity_cause=mp.inactivity_cause,
                inactivity_description=mp.inactivity_description,
            )
        )

    return ScrapedMpsResult(
        mps=scraped_mps,
        mp_to_term_links=mp_to_term_links,
    )


@dataclass
class ScrapedVotesResult:
    votes: list[database.VoteRecord]
    voting_options: list[database.VotingOption]


async def scrape_votes(
    client: httpx.AsyncClient,
    term: database.Term,
    sitting: database.Sitting,
    voting: database.Voting,
) -> ScrapedVotesResult:
    """Scrape individual MP votes for a specific voting.

    Handles both single-option and multiple-option votings. Also returns
    VotingOption records derived from the detail endpoint to handle
    inconsistencies between the list and detail API endpoints (e.g.
    duplicate voting numbers in older terms).

    Args:
        client: HTTP client instance.
        term: Term database model.
        sitting: Sitting database model.
        voting: Voting database model to scrape votes for.

    Returns:
        Scraped vote records and voting options from the detail endpoint.

    Raises:
        ValueError: If vote data is inconsistent (VOTE_VALID without
            multiple option votes).
    """
    voting_with_votes = await api_client.fetch_votes(
        client=client,
        term=term.number,
        sitting=sitting.number,
        voting=voting.number,
    )

    votes = voting_with_votes.mp_votes

    # Build VotingOptions from the detail endpoint response. This
    # ensures correct options exist even when the list endpoint
    # disagrees (e.g. duplicate voting numbers in older terms).
    scraped_voting_options: list[database.VotingOption] = []
    if voting_with_votes.voting_options is not None:
        for voting_option in voting_with_votes.voting_options:
            scraped_voting_options.append(
                database.VotingOption(
                    id=database_key_utils.generate_voting_option_natural_key(
                        term=term,
                        sitting=sitting,
                        voting=voting,
                        voting_option_index=voting_option.index,
                    ),
                    voting_id=voting.id,
                    index=voting_option.index,
                    option_label=voting_option.option_label,
                    description=voting_option.description,
                    votes=voting_option.votes,
                )
            )
    else:
        scraped_voting_options.append(
            database.VotingOption(
                id=database_key_utils.generate_voting_option_natural_key(
                    term=term,
                    sitting=sitting,
                    voting=voting,
                    voting_option_index=api_schemas.OptionIndex(1),
                ),
                voting_id=voting.id,
                index=api_schemas.OptionIndex(1),
                option_label="Default option (no options provided)",
                description=None,
                votes=0,
            )
        )

    scraped_votes: list[database.VoteRecord] = []

    for vote in votes:
        if vote.multiple_option_votes is None:
            if vote.vote == "VOTE_VALID":
                msg = (
                    "Invalid vote data: 'multiple_option_votes' "
                    "is None but 'vote' is 'VOTE_VALID'"
                )
                raise ValueError(msg)

            # Single option vote with default vote option
            scraped_votes.append(
                database.VoteRecord(
                    id=database_key_utils.generate_vote_natural_key(
                        term=term,
                        sitting=sitting,
                        voting=voting,
                        voting_option_index=api_schemas.OptionIndex(1),
                        mp_term_id=vote.mp_term_id,
                    ),
                    voting_option_id=database_key_utils.generate_voting_option_natural_key(
                        term=term,
                        sitting=sitting,
                        voting=voting,
                        voting_option_index=api_schemas.OptionIndex(1),
                    ),
                    mp_term_id=vote.mp_term_id,
                    vote=vote.vote,
                    party=vote.party,
                )
            )
        else:
            # Multiple options vote
            for voting_option, inner_vote in vote.multiple_option_votes.items():
                scraped_votes.append(
                    database.VoteRecord(
                        id=database_key_utils.generate_vote_natural_key(
                            term=term,
                            sitting=sitting,
                            voting=voting,
                            voting_option_index=voting_option,
                            mp_term_id=vote.mp_term_id,
                        ),
                        voting_option_id=database_key_utils.generate_voting_option_natural_key(
                            term=term,
                            sitting=sitting,
                            voting=voting,
                            voting_option_index=voting_option,
                        ),
                        mp_term_id=vote.mp_term_id,
                        vote=inner_vote,
                        party=vote.party,
                    )
                )

    return ScrapedVotesResult(
        votes=scraped_votes,
        voting_options=scraped_voting_options,
    )
