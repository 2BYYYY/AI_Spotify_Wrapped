import spotipy
import os
import pandas as pd
import json
# from sql_main import connect_to_sql
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
from sql_main import connect_to_sql

load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
# delete the cache before getting the refresh token
REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")
# The redirect URL in Spotipy is required for OAuth authentication. 
# After user consent, Spotify sends an authorization code to this URL, which is then exchanged for an access token to authenticate API requests.
REDIRECT_URL = "https://example.com/callback"

# Scopes ensure that Spotify users sharing data with third-party apps only share the information they consent to.
# more on scopes in sotify documentation (https://developer.spotify.com/documentation/web-api/concepts/scopes)
SCOPE = "user-top-read"

def main():
    auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URL,
        scope=SCOPE,
        
    )
    try:
        auth_manager.refresh_access_token(REFRESH_TOKEN)
        access_token = auth_manager.get_access_token(as_dict=False)
        sp = spotipy.Spotify(auth=access_token)
    # (EXCEPTION) SpotifyException via spotipy documentation
    except spotipy.SpotifyException as e:
        print(f"Error refreshing access token: {e}")
        # Handle token expiration or revocation here
        # For example, you can log the error and exit the script
        raise

    # returns the json
    # short_term → last 4 weeks
    # medium_term → last 6 months
    # long_term → all time
    top_tracks = sp.current_user_top_tracks(limit=10, time_range="short_term")
    top_artist = sp.current_user_top_artists(limit=10, time_range="short_term")
    # Formatting API response
    formatted_json_tracks = json.dumps(top_tracks, indent=4)  
    formatted_json_artist = json.dumps(top_artist, indent=4)  
    # November 27, 2024
    # Web API use cases will no longer be able to access or use the following endpoints and functionality in their third-party applications. 
    # audio_features = sp.audio_features(tracks=[track["id"] for track in top_tracks["items"]])

    # dataframe
    track_data = []
    for track, artist in zip(top_tracks["items"],top_artist["items"]):
        # For top tracks
        track_name = track["name"]
        # The reason with the slice [0] is because the json was a list
        track_artist_name = track["artists"][0]["name"]

        # For top artists
        artist_name = artist["name"]
        track_data.append({"top_track_name": f"({track_artist_name}) {track_name}",
                        "top_artist_name": artist_name})
        connect_to_sql(track_name, track_artist_name, 1, artist_name, 1)
    df = pd.DataFrame(track_data)   
    # via the volumes (initialize the volumes first in the .yaml file to show the folder)
    df.to_csv("/opt/airflow/PROJECT_Spotipy/wrapped-grind-2025.csv", index=False)

if __name__ == "__main__":
    main()