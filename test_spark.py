import os
from pyspark.sql import SparkSession
import json
from datetime import date

# Set environment variables (only necessary if not set system-wide)
os.environ['JAVA_HOME'] = "C:\\Program Files\\Java\\jdk-17"
os.environ['HADOOP_HOME'] = 'C:/Users/desag/OneDrive/Documents/A2/advance_database_and_big_data/hadoop-3.0.0'
os.environ['SPARK_HOME'] = 'C:/Users/desag/OneDrive/Documents/A2/advance_database_and_big_data/spark'


# Initialize Spark session
spark = SparkSession.builder \
    .appName("Format and Combine Data") \
    .getOrCreate()

# Define paths
current_day = date.today().strftime("%Y%m%d")
base_path = "datalake"

tmdb_raw_path = os.path.join(base_path, f"C:/Users/desag/OneDrive/Documents/A2/advance_database_and_big_data/big_data_project/datalake/raw/tmdb/now_playing/{current_day}/now_playing.json")
tmdb_formatted_path = os.path.join(base_path, f"C:/Users/desag/OneDrive/Documents/A2/advance_database_and_big_data/big_data_project/datalake/formatted/tmdb/NowPlaying/{current_day}")

tvmaze_raw_path = os.path.join(base_path, f"C:/Users/desag/OneDrive/Documents/A2/advance_database_and_big_data/big_data_project/datalake/raw/tvmaze/new_series/{current_day}/new_series.json")
tvmaze_formatted_path = os.path.join(base_path, f"C:/Users/desag/OneDrive/Documents/A2/advance_database_and_big_data/big_data_project/datalake/formatted/tvmaze/new_series/{current_day}")

combined_path = os.path.join(base_path, f"C:/Users/desag/OneDrive/Documents/A2/advance_database_and_big_data/big_data_project/datalake/formatted/combined/series_movies/{current_day}")

# Function to standardize entries
def standardize_entry(entry, is_series):
    if is_series:
        show = entry.get("show", {})
        rating = show.get("rating", {})
        image = show.get("image", {})
        return {
            "title": show.get("name"),
            "release_date": entry.get("airdate"),
            "overview": entry.get("summary") or show.get("summary"),
            "popularity": rating.get("average") or show.get("weight"),
            "vote_average": rating.get("average"),
            "vote_count": None,
            "image": image.get("original") or image.get("medium") if image else None,
            "language": show.get("language"),
            "genres": show.get("genres"),
            "id": entry.get("id")
        }
    else:
        return {
            "title": entry.get("title") or entry.get("original_title"),
            "release_date": entry.get("release_date"),
            "overview": entry.get("overview"),
            "popularity": entry.get("popularity"),
            "vote_average": entry.get("vote_average"),
            "vote_count": entry.get("vote_count"),
            "image": f"https://image.tmdb.org/t/p/original{entry.get('poster_path')}" if entry.get("poster_path") else None,
            "language": entry.get("original_language"),
            "genres": entry.get("genre_ids"),
            "id": entry.get("id")
        }

# Load raw JSON files
with open(tvmaze_raw_path, 'r') as file:
    new_series = json.load(file)

with open(tmdb_raw_path, 'r') as file:
    now_playing = json.load(file)

# Standardize the entries in both new_series and now_playing
standardized_new_series = [standardize_entry(entry, True) for entry in new_series]
standardized_now_playing = [standardize_entry(entry, False) for entry in now_playing['results']]

# Combine the standardized entries
standardized_combined_json = {
    "new_series": standardized_new_series,
    "now_playing": standardized_now_playing
}

# Save the standardized JSON files
os.makedirs(tmdb_formatted_path, exist_ok=True)
os.makedirs(tvmaze_formatted_path, exist_ok=True)
os.makedirs(combined_path, exist_ok=True)

tmdb_output_path = os.path.join(tmdb_formatted_path, "now_playing.json")
tvmaze_output_path = os.path.join(tvmaze_formatted_path, "new_series.json")
combined_output_path = os.path.join(combined_path, "combined_series_movies.json")

with open(tmdb_output_path, 'w') as file:
    json.dump(standardized_now_playing, file, indent=4)

with open(tvmaze_output_path, 'w') as file:
    json.dump(standardized_new_series, file, indent=4)

with open(combined_output_path, 'w') as file:
    json.dump(standardized_combined_json, file, indent=4)

# Stop the Spark session
spark.stop()

print(tmdb_output_path)
print(tvmaze_output_path)
print(combined_output_path)
