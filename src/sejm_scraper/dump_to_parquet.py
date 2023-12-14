import pandas as pd

import database as db

if __name__ == "__main__":
    for table in ["Votings", "PartyVotesLinks", "Votes"]:
        df = pd.read_sql_table(table, db.engine)
        df.to_parquet(f"{table}.parquet")
