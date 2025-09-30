import os
import pandas as pd

from dotenv import load_dotenv
load_dotenv()

from sqlalchemy import create_engine, text


SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_HOST = os.getenv("SQL_HOST")
SQL_DB = os.getenv("SQL_DB")
# Normalized already No Transative and Partial dependencies
SQL_QUERY_ARTIST = os.getenv("SQL_QUERY_ARTIST")
SQL_QUERY_TRACK = os.getenv("SQL_QUERY_TRACK")
ENGINE_LINK = f"mysql+pymysql://{SQL_USER}:{SQL_PASSWORD}@{SQL_HOST}/{SQL_DB}"

engine = create_engine(ENGINE_LINK)

def check_engine_connection():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            # a single value (first column, first row)
            print(f"[CONNECTED] test_engine_connection value: {result.scalar()}")
            return True
    except Exception as e:
        print(f"[NOT CONNECTED] test_engine_connection: {e}")
        raise

def connect_to_sql(top_tracks: str, top_tracks_artist: str, count_tracks: int, top_artist: str, count_artist: int):
    try:
        # same as connect(), but starts a transaction automatically and commits/rolls back when the block ends.
        with engine.begin() as conn:
            print("Connected to DB")

            conn.execute(
                # if there are dups update else insert
                text(SQL_QUERY_ARTIST),
                # to avoid sql injects
                {
                    "artist_name": top_artist, 
                    "artist_count": count_artist
                }
            )

            conn.execute(
                text(SQL_QUERY_TRACK),
                {
                    "artist_name": top_tracks_artist,
                    "track_name": top_tracks,
                    "count_tracks": count_tracks
                }
            )
            print("[SUCCESSFUL] connect_to_sql")

    except Exception as e:
        print("[NOT SUCCESSFUL] connect_to_sql:", e)
        raise
        
if __name__ == "__main__":
    connect_to_sql()