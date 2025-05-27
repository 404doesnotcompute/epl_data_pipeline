#boiler plate code needed for the logger to work
import sys,os
from utilities.logger import get_logger
logger = get_logger(__name__)

import boto3 
from datetime import datetime
from io import StringIO
import json

class S3Wrapper:
    def __init__(self,aws_access_key_id,aws_secret_access_key,region,bucket):
        self.bucket = bucket
        self.s3_client = boto3.client(
        's3',
        aws_access_key_id = aws_access_key_id,
        aws_secret_access_key = aws_secret_access_key,
        region_name = region
        )
        self.dt_format = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    
    def s3_upload(self,source_path,file_name):

        try:
            self.s3_client.upload_file(source_path,self.bucket,f"{self.dt_format}_{file_name}")
            logger.info(f"Object successfully uploaded to: {self.bucket}")
        except Exception as e:
            logger.exception(f"Exception occured at s3_upload: {e}")
            return None
    
    def s3_download(self,object_name,landing_path):
        
        try:
            self.s3_client.download_file(self.bucket,object_name,landing_path)
            logger.info(f"Object successfully downloaded to: {landing_path}")
        except Exception as e:
            logger.exception(f"Exception occured at s3_download: {e}")
            return None
    
    def s3_object_checker(self,object_name):
        try:
            self.s3_client.head_object(Bucket=self.bucket,Key=object_name)
            logger.info(f"Object found: {object_name} in s3 bucket: {self.bucket}")
            return True
        except Exception as e:
            logger.warning(f"Object name: {object_name} not found in {self.bucket}: {e}")
            return False
    
    def s3_upload_buffer(self, buffer:StringIO, s3_key):
        """
        Uploads a string buffer (CSV, JSON, or text) to S3 under the specified key.
        The buffer must be seeked to position 0 before calling this method.
        """
        try:
            self.s3_client.put_object(
                Bucket=self.bucket, 
                Key=f"{self.dt_format}_{s3_key}", 
                Body=buffer.getvalue()
                )
            logger.info(f"Object successfully uploaded to: {self.bucket}")
            
        except Exception as e:
            logger.warning(f"Exception occured at s3_upload_buffer: {e}")
            return False
    
    def s3_upload_raw_json(self, data:dict, s3_key, include_ts=False):
        try:
            buffer = StringIO()
            json.dump(data,buffer)
            buffer.seek(0)

            key = f"{self.dt_format}_{s3_key}" if include_ts else s3_key

            self.s3_upload_buffer(buffer,key)
            logger.info(f"Raw JSON uploaded to: {key}")
            return True
        except Exception as e:
            logger.warning(f"Failed to upload raw JSON to: {key}")
            return False

        