#utility and wrapper imports
from utilities.logger import get_logger
from src.api_ingestor import APIIngestion
from src.data_cleaner import DataCleaner
from src.qa_checker import DataQualityChecker
from src.s3_wrapper import S3Wrapper
from src.postgres_wrapper import PostgresWrapper

#library imports
import os,dotenv
from io import StringIO
import time
import pandas as pd

#setting up logger and loading .env
logger = get_logger(__name__)
dotenv.load_dotenv()

def fetch_all_players(api,s3, team_ids):
    all_players = []
    for team_id in team_ids:
        player_df = api.fetch("epl/v1/teams", team_id=team_id)
        if player_df is not None:
            player_df["team_id"] = team_id
            all_players.append(player_df)

            #Step 3B: Upload raw JSON for each team's players
            s3.s3_upload_raw_json(
                data=player_df.to_dict(orient="records"),
                s3_key=f"raw/json/epl/players/{team_id}_players.json",
                include_ts=False
            )
        time.sleep(12)  # obey free-tier API limit    
    return pd.concat(all_players, ignore_index=True)

def main():
    logger.info("Pipeline Starting...")

    #step 1: Set up clients
    api = APIIngestion(
        base_url='https://api.balldontlie.io',
        season=2024,
        headers={
            "Authorization": os.getenv('API_KEY')
            }
        )
    
    pg = PostgresWrapper(
        db_name=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host='localhost',
        port=5432
    )

    s3 = S3Wrapper(
        os.getenv('AWS_ACCESS_KEY'),
        os.getenv('AWS_SECRET_KEY'),
        os.getenv('AWS_REGION'),
        't1-de-prep'
        )
    
    #step 2 ingest Teas from API 
    teams_df = api.fetch('epl/v1/teams')
    if teams_df is None:
        logger.error("Failed to fetch team data")
        return
    
    #Step 3A: Upload raw team JSON
    s3.s3_upload_raw_json(
        data=teams_df.to_dict(orient="records"),
        s3_key='raw/json/teams/2024_teams.json',
        include_ts=False
    )

    #Step 4: Clean teams data
    teams_cleaner = DataCleaner(teams_df)
    teams_clean = teams_cleaner.drop_nulls()
    teams_clean = teams_cleaner.lower_columns()
    teams_clean = teams_cleaner.fill_nulls()

    #Step 5: Load clean teams data to Postgres
    pg.copy_from_df(teams_clean,"epl_datapipeline.epl_teams")
    logger.info("Teams successfully loaded.")
    
    # Step 6: Ingest & upload players raw per team
    team_ids = teams_clean["id"].tolist()
    players_df = fetch_all_players(api, s3, team_ids)

    # Step 7: Clean and load players
    players_cleaner = DataCleaner(players_df)
    players_clean = players_cleaner.drop_nulls()
    players_clean = players_cleaner.lower_columns()
    players_clean = players_cleaner.fill_nulls()

    expected_columns = [
    'id', 'position', 'national_team', 'height', 'weight',
    'birth_date', 'age', 'name', 'first_name', 'last_name', 'team_id'
    ]

    players_clean = players_clean[expected_columns]

    pg.copy_from_df(players_clean, "epl_datapipeline.epl_team_players")
    logger.info("Players successfully loaded.")

    pg.close()
    logger.info("Pipeline Complete!")

if __name__ == "__main__":
    main()
