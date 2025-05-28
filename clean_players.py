from utilities.logger import get_logger
from src.data_cleaner import DataCleaner

logger = get_logger(__name__)

def clean_players(players_df):  
    if players_df is None or players_df.empty:
        logger.warning("Input DataFrame is empty or None. Skipping cleaning.")
        return players_df
    
    logger.info(f"Cleaning players DataFrame with {len(players_df)} rows.")

    cleaner = (
        DataCleaner(players_df)
        .drop_nulls()
        .lower_columns()
        .cast_column_types({
            "height": "int",
            "weight": "int"
        })
        .fill_nulls()
        )
    
    cleaned_players = cleaner.get_df()

    expected_columns = [
    'id', 'position', 'national_team', 'height', 'weight',
    'birth_date', 'age', 'name', 'first_name', 'last_name', 'team_id'
    ]
    cleaned_players = cleaned_players[expected_columns]

    logger.info(f"Cleaning complete. Output rows: {len(cleaned_players)}")



    return cleaned_players

