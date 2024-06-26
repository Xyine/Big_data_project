# fetch_tmdb_data.py
import requests
import os
import json
from datetime import date

# Replace 'your_tmdb_api_key' with your actual TMDB API key
API_KEY = '4ed0acf1aad0ecac8d44376386da0cc7'
BASE_URL = 'https://api.themoviedb.org/3'

def fetch_tmdb_data():
    endpoint = f'{BASE_URL}/movie/now_playing?api_key={API_KEY}'  # Corrected the endpoint to 'now_playing'
    response = requests.get(endpoint)
    
    if response.status_code != 200:
        print("Error:", response.json())
        return
    
    data = response.json()

    current_day = date.today().strftime("%Y%m%d")
    DATALAKE_ROOT_FOLDER = os.path.join(f"/home/xyine/code_project/big_data_project")
    TARGET_PATH = os.path.join(DATALAKE_ROOT_FOLDER, "datalake/raw/tmdb/now_playing", current_day)  # Adjusted the path to match the corrected endpoint
    os.makedirs(TARGET_PATH, exist_ok=True)  # Using exist_ok=True to avoid errors if the directory already exists
    file_path = os.path.join(TARGET_PATH, "now_playing.json")  # Adjusted the filename to match the corrected endpoint
    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)

# To be used in Airflow
def fetch_tmdb_data_task(**kwargs):
    fetch_tmdb_data()
