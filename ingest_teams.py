import pandas as pd
from utilities.logger import get_logger

logger = get_logger(__name__)

def ingest_teams_and_store_all_teams(api,s3):
    """
    This function does two main things:
    
    1.  Gets all teams via 'epl/v1/teams' endpoint, stores the results in a dataframe.

    2.  Then uploads the raw json from the API call into s3 for storage.
    """
    logger.info("Teams ingestion starting....")
    try:
        teams_df = api.fetch('epl/v1/teams')
        if teams_df is not None and not teams_df.empty:
            s3.s3_upload_raw_json(
                data=teams_df.to_dict(orient="records"),
                s3_key='raw/json/teams/2024_teams.json',
                include_ts=False
            )
            logger.info(f"Fetched and uploaded all team data to s3")
        else:
            logger.warning(f"No teams data returned or DataFrame is empty")
    except Exception as e:
        logger.warning(f"Error processing teams data") 
    
    logger.info("Teams ingestion complete!")

    return teams_df
        