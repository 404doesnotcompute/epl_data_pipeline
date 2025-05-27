"""
- Handles Ingestion from API and normalization of JSON data
"""
import sys,os
import requests 
from utilities.logger import get_logger
import pandas as pd
import json

logger = get_logger(__name__)

class APIIngestion:
    def __init__(self,base_url,season,headers=None):
        self.base_url = base_url.rstrip('/')
        self.season = str(season)
        self.headers = headers
    
    def build_url(self,endpoint, team_id=None):
        if team_id:
            return (f"{self.base_url}/{endpoint.strip('/')}/{team_id}/players?season={self.season}")
        else:
            return f"{self.base_url}/{endpoint.strip('/')}?season={self.season}"      
        
    def fetch(self, endpoint,team_id=None):
        """
        Builds full request URL and returns normalized DataFrame from API response.
        Example: endpoint='epl/v1'
        """
        logger.info("API Ingestion started...")
        url = self.build_url(endpoint,team_id)
        logger.debug(f"Request URL: {url}")
        
        try:
            response = requests.get(url, headers = self.headers)

            if response.status_code == 200:
                logger.info("API Validation Successful!")
                data = response.json()
                return pd.json_normalize(data['data'])
            else:
                logger.warning(f"Error: {response.status_code}")
                logger.warning(f"Response Body: {response.text}")
        except Exception as e:
            logger.exception(f"Exception during API ingestion: {e}")

        logger.info("API Ingestion Complete!")
        return None

