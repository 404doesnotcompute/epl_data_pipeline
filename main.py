"""
main.py

This script runs an end-to-end ETL pipeline:
- Ingests EPL team and player data from API
- Uploads raw JSON to AWS S3
- Cleans data
- Uploads cleaned data to PostgreSQL
"""


#utility and wrapper imports
from utilities.logger import get_logger
from src.api_ingestor import APIIngestion
from src.s3_wrapper import S3Wrapper
from src.postgres_wrapper import PostgresWrapper
from clean_players import clean_players
from clean_teams import clean_teams
from ingest_players import fetch_and_store_all_players_raw
from ingest_teams import ingest_teams_and_store_all_teams
from pg_upload import pg_upload

#library imports
import os
import dotenv
import pandas as pd

#setting up logger and loading .env
logger = get_logger(__name__)
dotenv.load_dotenv()


def main():
    # Instantiate wrappers (api, s3, pg)
    api = APIIngestion(
        base_url='https://api.balldontlie.io/',
        season=2024,
        headers= {"Authorization": f"Bearer {os.getenv('API_KEY')}"}                                   
        )

    s3 = S3Wrapper(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
        region=os.getenv('AWS_REGION'),
        bucket='t1-de-prep')
    
    pg = PostgresWrapper(
        db_name=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host='localhost',
        port=5432
        )

    # Ingest teams
    teams_df = ingest_teams_and_store_all_teams(api, s3)
    cleaned_teams = clean_teams(teams_df)
    pg_upload(pg, cleaned_teams, "epl_datapipeline.epl_teams")

    # Ingest players
    team_ids = cleaned_teams["id"].tolist()
    players_df = fetch_and_store_all_players_raw(api, s3, team_ids)
    cleaned_players = clean_players(players_df)
    pg_upload(pg, cleaned_players, "epl_datapipeline.epl_team_players")

    # Close connection
    pg.close()
    logger.info("ETL pipeline completed successfully!")

if __name__ == "__main__":
    main()