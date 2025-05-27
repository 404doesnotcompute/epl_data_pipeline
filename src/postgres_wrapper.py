import sys,os
from utilities.logger import get_logger
import psycopg2
from psycopg2.extras import RealDictCursor
from io import StringIO

logger = get_logger(__name__)

class PostgresWrapper:
    def __init__(self,db_name,user,password,host='localhost',port=5432):
        try:
            self.conn = psycopg2.connect(
                    dbname=db_name,
                    user=user,
                    password=password,
                    host=host,
                    port=port
            )
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info("Connection to PostgreSQL succesful!")
        except Exception as e:
            logger.warning(f"Failed to connect to PostgreSQL: {e}")

    def __enter__(self):
        return self
    
    def __exit__(self,exc_type, exc_val, exc_tb):
        self.close()

    def run_query(self,query,params=None):
        #returns data from SELECT
        try:
            self.cursor.execute(query,params)
            logger.info("Query successfully completed!")
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Query failed: {e}")
            return None

    def run_command(self,command,params=None):
        #used for INSERT, UPDATE, DDL, etc..
        try:
            self.cursor.execute(command,params)
            self.conn.commit()
            logger.info("Comand executed and commited!")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Command failed and rolled back: {e}")

    def copy_from_csv(self,file_path,table_name):
        try:
            with open(file_path,'r') as f:
                self.cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER",f) 
            self.conn.commit()
            logger.info(f"CSV Table loaded into table: {table_name}")
        except Exception as e:
            self.conn.rollback()
            logger.warning(f"CSV load failed: {e}")

    def export_to_csv(self,file_path,table_name):
        try:
            with open(file_path,'w') as f:
                self.cursor.copy_expert(f"COPY {table_name} TO STDOUT WITH CSV HEADER",f) 
            logger.info(f"CSV exported to file path: {file_path}")
        except Exception as e:
            logger.warning(f"CSV export failed: {e}")

    def copy_from_df(self,df,table):
        try:
            #saves dataframe to memory
            buffer = StringIO()
            df.to_csv(buffer, header=False,index=False)
            buffer.seek(0)

            self.cursor.copy_expert(
                f"COPY {table} FROM STDIN WITH CSV",buffer
            )

            self.conn.commit()
            logger.info(f"Data inserted into: {table}")
        except Exception as e:
            logger.warning(f"Data export failed: {e}")
            return None

    def close(self):
        try:
            self.cursor.close()
            self.conn.close()
            logger.info("PostgreSQL Connection is closed!")
        except Exception as e:
            logger.warning(f"There was trouble closing the connection: {e}")
            return None