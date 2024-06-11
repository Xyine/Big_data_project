import requests
import os
import json
from datetime import date

# Base URL for TVmaze API
BASE_URL = 'https://api.tvmaze.com'


def fetch_tvmaze_data():
    # Fetch the schedule for a given country (default is US)
    response = requests.get(f'{BASE_URL}/schedule')

    if response.status_code == 200:
        try:
            data = response.json()
        except requests.exceptions.JSONDecodeError:
            return  # Exit the function if JSON decoding fails
    else:
        print("Failed to fetch new series data")
        return

    current_day = date.today().strftime("%Y%m%d")
    TARGET_PATH = f"C:/Users/desag/OneDrive/Documents/A2/advance_database_and_big_data/big_data_project/datalake/raw/tvmaze/new_series/{current_day}"
    os.makedirs(TARGET_PATH, exist_ok=True)  # Using exist_ok=True to avoid errors if the directory already exists

    file_path = os.path.join(TARGET_PATH, "new_series.json")
    with open(file_path, "w") as f:
        json.dump(data, f, indent=4)



if __name__ == "__main__":
    fetch_tvmaze_data()
