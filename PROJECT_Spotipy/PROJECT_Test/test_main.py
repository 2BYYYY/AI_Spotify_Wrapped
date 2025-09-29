# run the file as a module "python -m"
from PROJECT_Spotipy.sql_main import check_engine_connection
from PROJECT_Spotipy.spoti_main import spotipy_initialization

def test_engine_connection():
    assert check_engine_connection() is True
def test_spotipy_initialization():
    me = spotipy_initialization()
    user = me.current_user()
    assert user["external_urls"]["spotify"] == "https://open.spotify.com/user/odu3ro26f7yhha2gkomh0tdq0"
    assert user["id"] == "odu3ro26f7yhha2gkomh0tdq0"

if __name__ == "__main__":
    test_engine_connection()
    test_spotipy_initialization()