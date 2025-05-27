#boiler plate code needed for the logger to work
import sys,os
from utilities.logger import get_logger
logger = get_logger(__name__)

import pandas as pd

class DataQualityChecker:
    def __init__(self,df):
        self.df = df
    
    def check_nulls(self):
        try:
            nulls_by_column = self.df.isnull().sum()
            nulls_by_column = nulls_by_column[nulls_by_column >0]

            if not nulls_by_column.empty:
                logger.warning(f"There are nulls in the data: {nulls_by_column}")
            else:
                logger.info("There are no nulls in the data set")
            return nulls_by_column
        except Exception as e:
            logger.exception(f"Exception in check_nulls: {e}")
            return None 

    def check_duplicates(self):
        try:
            dupes_count = self.df.duplicated().sum() 

            if dupes_count > 0:
                logger.warning(f"There are duplicates in the data: {dupes_count}")
            else:
                logger.info("There are no duplicates in the data set")
            return dupes_count
        except Exception as e:
            logger.exception(f"Exception in check_duplicates: {e}")
            return None


    def check_schema(self,expected_columns):
        try:
            actual_columns = set(self.df.columns)
            expected_columns = set(expected_columns)

            if actual_columns != expected_columns:
                missing = expected_columns - actual_columns
                extra = actual_columns - expected_columns
                if missing:
                    logger.warning(f"Missing columns: {missing}")
                if extra:
                    logger.warning(f"Unexpected columns: {extra}")
                return {"missing": missing, "extra": extra} if missing or extra else actual_columns
            else:
                logger.info("No schema drift detected.")
            return actual_columns
        except Exception as e:
            logger.exception(f"Exception in check_schema: {e}")
            return None
    
    def check_data_types(self,expected_dtypes):
        try:
            actual_dtypes = {
                col: dtype.name for col, dtype in self.df.dtypes.items()
            }

            mismatched = {}

            for col,expected_type in expected_dtypes.items():
                actual_type = actual_dtypes.get(col)
                if actual_type != expected_type:
                    mismatched[col] = {
                        "expected": expected_type,
                        "actual": actual_type
                        }
            if mismatched:
                logger.warning(f"Mismatched data types: {mismatched}")
            else:
                logger.info(f"All column data types match expected types.")
            return mismatched if mismatched else actual_dtypes
        
        except Exception as e:
            logger.exception(f"Exception in check_data_types: {e}")
            return None

    