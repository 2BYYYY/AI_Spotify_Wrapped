import os
import pandas as pd

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text


SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_HOST = os.getenv("SQL_HOST")
SQL_DB = os.getenv("SQL_DB")
ENGINE_LINK = f"mysql+pymysql://{SQL_USER}:{SQL_PASSWORD}@{SQL_HOST}/{SQL_DB}"

engine = create_engine(ENGINE_LINK)

# Normalized already No Transative and Partial dependencies
QUERY = """
            SELECT *
            FROM table
        """

def connect_to_sql():
    try:
        with engine.connect() as conn:
            print("Connected to DB")
            # dataframe to have tabular structure 
            df = pd.read_sql(text(QUERY), conn)
            print(df)
    except Exception as e:
        print("Connection failed:", e)
        
print(__name__)