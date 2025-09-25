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

def connect_to_sql(top_tracks, top_tracks_artist, count_tracks, top_artist, count_artist):
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
            print("successful for artist and track")

    except Exception as e:
        print("Connection failed:", e)
        
if __name__ == "__main__":
    connect_to_sql()