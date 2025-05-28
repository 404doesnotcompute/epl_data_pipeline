#boiler plate code needed for the logger to work
import sys,os
from utilities.logger import get_logger
logger = get_logger(__name__)

import pandas as pd
import datetime as dt
from datetime import date

class DataCleaner:
    def __init__(self,df):
        self.df = df

    def drop_nulls(self):
        self.df = self.df.dropna()
        return self

    def fill_nulls(self):
        self.df = self.df.fillna('')
        return self
    
    def drop_columns(self,columns_to_drop=None):
        if columns_to_drop is None:
            logger.warning(f"No columns specified to drop.")
        else:
            self.df = self.df.drop(columns=columns_to_drop)
        return self
    
    def lower_columns(self):
        self.df.columns = self.df.columns.str.lower()
        return self
    
    def date_string_conversion(self,col):
        self.df[col] = pd.to_datetime(self.df[col],errors="coerce")
        self.df[col] = self.df[col].dt.strftime('%Y-%m-%d')
        return self
    
    def datetime_conversion(self,col):
        self.df[col] = pd.to_datetime(self.df[col],errors="coerce")
        return self
    
    def rename_columns(self,rename_map):
        if not isinstance(rename_map, dict):
            logger.warning("rename_columns() expects a dictionary")
            return self.df
        self.df.rename(columns=rename_map, inplace=True)
        return self
    
    def cast_column_types(self, type_map):
        if not isinstance(type_map, dict):
            logger.warning("cast_column_types expectd a dictionary")
            return self.df

        for col, dtype in type_map.items():
            if col not in self.df.columns:
                logger.warning(f"Column '{col}' not found in DataFrame")
                continue
            try:
                self.df[col] = self.df[col].astype(dtype)
                logger.info(f"Cast column '{col}' to {dtype}.")
            except Exception as e:
                logger.warning(f"Could not cast column '{col}' to {dtype}: {e}")
        return self
    
    def clean_age_column(self, col='age'):
        if col in self.df.columns:
            self.df[col] = self.df[col].str.extract(r'(\d+)').astype(float)
        else:
            logger.warning(f"Column '{col}' not found for age cleaning.")
        return self
    
    def get_df(self):
        return self.df