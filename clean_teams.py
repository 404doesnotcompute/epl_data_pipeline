from utilities.logger import get_logger
from src.data_cleaner import DataCleaner

logger = get_logger(__name__)

def clean_teams(teams_df):  
    if teams_df is None or teams_df.empty:
        logger.warning("Input DataFrame is empty or None. Skipping cleaning.")
        return teams_df
    
    logger.info(f"Cleaning teams DataFrame with {len(teams_df)} rows.")

    cleaned_df = (
        DataCleaner(teams_df)
        .drop_nulls()
        .lower_columns()
        .fill_nulls()
        .get_df()
        )
    
    logger.info(f"Cleaning complete. Output rows: {len(cleaned_df)}")
    return cleaned_df
