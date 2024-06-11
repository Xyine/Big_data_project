import os
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Define paths
current_day = date.today().strftime("%Y%m%d")
base_path = r"C:\Users\desag\OneDrive\Documents\A2\advance_database_and_big_data\big_data_project\datalake"

tmdb_formatted_path = os.path.join(base_path, f"formatted\\tmdb\\NowPlaying\\{current_day}\\now_playing.json")
tvmaze_formatted_path = os.path.join(base_path, f"formatted\\tvmaze\\new_series\\{current_day}\\new_series.json")

tmdb_parquet_path = os.path.join(base_path, f"parquet\\tmdb\\NowPlaying\\{current_day}")
tvmaze_parquet_path = os.path.join(base_path, f"parquet\\tvmaze\\new_series\\{current_day}")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("JSON to Parquet") \
    .getOrCreate()

# Function to print the content of the JSON file
def print_json_content(json_path):
    try:
        with open(json_path, 'r') as file:
            data = file.read()
            print(f"Content of {json_path}:")
            print(data[:1000])  # Print first 1000 characters for brevity
    except Exception as e:
        print(f"Error reading {json_path}: {e}")

# Function to convert JSON to Parquet
def json_to_parquet(json_path, parquet_path):
    try:
        # Print JSON content for debugging
        print_json_content(json_path)

        # Read JSON file into Spark DataFrame without specifying schema
        df = spark.read.option("multiline", "true").json(json_path)
        
        # Print inferred schema and data to debug
        df.printSchema()
        df.show(truncate=False)
        
        # Filter out corrupt records if present
        if '_corrupt_record' in df.columns:
            df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
        
        # Check if DataFrame is empty
        if df.head(1):
            # Write DataFrame to Parquet
            df.write.mode("overwrite").parquet(parquet_path)
            print(f"Data from {json_path} saved to {parquet_path}")
        else:
            print(f"No valid data found in {json_path}. Skipping conversion to Parquet.")
    except Exception as e:
        print(f"Error processing {json_path}: {e}")

# Convert TMDB formatted JSON to Parquet
json_to_parquet(tmdb_formatted_path, tmdb_parquet_path)

# Convert TVMaze formatted JSON to Parquet
json_to_parquet(tvmaze_formatted_path, tvmaze_parquet_path)

# Stop Spark session
spark.stop()
