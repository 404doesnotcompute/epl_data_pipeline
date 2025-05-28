import time
import pandas as pd
from utilities.logger import get_logger

logger = get_logger(__name__)

def fetch_and_store_all_players_raw(api,s3, team_ids):
    """
    This function does two main things:
    
    1.  It isolates the players API call from the teams API call. Additionally, 
        the players data result from the API does not include the team_id. The data is - 
        then returned to a clean dataframe with the team_id column created. 

    2.  The function uploads the raw json results to s3 and assigns each file it's respective
        team id.

    Notes: 
        "time.sleep(12)" is included to respect the request limit of the API(5 calls per minute).
        There are 20 teams total, if I looped through based on team_id without method, the script 
        will produce an error.  
    """
    logger.info("Players ingestion starting....")

    all_players = []
    for team_id in team_ids:
        try:
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
                logger.info(f"Fetched and uploaded player data for team ID: {team_id}")
            else:
                logger.warning(f"No data returned to team ID: {team_id}")
        except Exception as e:
            logger.warning(f"Error processing team ID {team_id}: {e}")
        time.sleep(15)  # obey free-tier API limit    
    
    if not all_players:
        logger.warning("No player data was fetched for any team.")
        return pd.DataFrame()
    
    logger.info("Players ingestion complete!")
    return pd.concat(all_players, ignore_index=True)