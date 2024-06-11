import os
import json
from datetime import date

# Define paths
current_day = date.today().strftime("%Y%m%d")
base_path = r"C:\Users\desag\OneDrive\Documents\A2\advance_database_and_big_data\big_data_project\datalake"

tmdb_raw_path = os.path.join(base_path, f"raw\\tmdb\\now_playing\\{current_day}\\now_playing.json")
tmdb_formatted_path = os.path.join(base_path, f"formatted\\tmdb\\NowPlaying\\{current_day}")

tvmaze_raw_path = os.path.join(base_path, f"raw\\tvmaze\\new_series\\{current_day}\\new_series.json")
tvmaze_formatted_path = os.path.join(base_path, f"formatted\\tvmaze\\new_series\\{current_day}")

# Function to standardize TMDB entries
def standardize_tmdb_entry(entry):
    return {
        "title": entry.get("title") or entry.get("original_title"),
        "release_date": entry.get("release_date"),
        "overview": entry.get("overview"),
        "popularity": float(entry.get("popularity")) if entry.get("popularity") else None,
        "vote_average": float(entry.get("vote_average")) if entry.get("vote_average") else None,
        "vote_count": int(entry.get("vote_count")) if entry.get("vote_count") else None,
        "image": f"https://image.tmdb.org/t/p/original{entry.get('poster_path')}" if entry.get("poster_path") else None,
        "language": entry.get("original_language"),
        "genres": entry.get("genre_ids"),
        "id": int(entry.get("id")) if entry.get("id") else None
    }

# Function to standardize TVMaze entries
def standardize_tvmaze_entry(entry):
    show = entry.get("show", {})
    rating = show.get("rating", {})
    image = show.get("image", {})
    return {
        "title": show.get("name"),
        "release_date": entry.get("airdate"),
        "overview": entry.get("summary") or show.get("summary"),
        "popularity": float(rating.get("average") or show.get("weight")) if rating.get("average") or show.get("weight") else None,
        "vote_average": float(rating.get("average")) if rating.get("average") else None,
        "vote_count": None,
        "image": image.get("original") or image.get("medium") if image else None,
        "language": show.get("language"),
        "genres": show.get("genres"),
        "id": entry.get("id")
    }

# Load raw JSON files
with open(tvmaze_raw_path, 'r') as file:
    new_series = json.load(file)

with open(tmdb_raw_path, 'r') as file:
    now_playing = json.load(file)

# Standardize the entries in both new_series and now_playing
standardized_new_series = [standardize_tvmaze_entry(entry) for entry in new_series]
standardized_now_playing = [standardize_tmdb_entry(entry) for entry in now_playing['results']]

# Create formatted paths if they do not exist
os.makedirs(tmdb_formatted_path, exist_ok=True)
os.makedirs(tvmaze_formatted_path, exist_ok=True)

# Paths to save the standardized JSON files
tmdb_formatted_file_path = os.path.join(tmdb_formatted_path, "now_playing.json")
tvmaze_formatted_file_path = os.path.join(tvmaze_formatted_path, "new_series.json")

# Save the standardized data to JSON files
try:
    with open(tmdb_formatted_file_path, 'w') as file:
        json.dump(standardized_now_playing, file, indent=4)
    
    with open(tvmaze_formatted_file_path, 'w') as file:
        json.dump(standardized_new_series, file, indent=4)
    
    print(f"Data saved to: {tmdb_formatted_file_path} and {tvmaze_formatted_file_path}")
except Exception as e:
    print(f"Error saving JSON: {e}")
