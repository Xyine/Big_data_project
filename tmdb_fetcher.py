import requests
import os
import json
from datetime import date

# Replace 'your_tmdb_api_key' with your actual TMDB API key
API_KEY = '4ed0acf1aad0ecac8d44376386da0cc7'
BASE_URL = 'https://api.themoviedb.org/3'


def fetch_tmdb_data():
    endpoint = f'{BASE_URL}/movie/NowPlaying?api_key={API_KEY}'
    response = requests.get(endpoint)
    data = response.json()

    current_day = date.today().strftime("%Y%m%d")
    DATALAKE_ROOT_FOLDER = os.path.join(f"C:/Users/desag/OneDrive/Documents/A2/advance_database_and_big_data/big_data_project")
    TARGET_PATH = os.path.join(DATALAKE_ROOT_FOLDER, "datalake/raw/tmdb/NowPlaying", current_day)

    print(f"Creating directory: {TARGET_PATH}")
    os.makedirs(TARGET_PATH, exist_ok=True)  # Using exist_ok=True to avoid errors if the directory already exists

    file_path = os.path.join(TARGET_PATH, "NowPlaying.json")
    print(f"Saving data to: {file_path}")
    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)

    print("Data has been successfully written.")


if __name__ == "__main__":
    fetch_tmdb_data()
