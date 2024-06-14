# combine_parquet.py
import os
from datetime import date
from pyspark.sql import SparkSession

# Define paths
current_day = date.today().strftime("%Y%m%d")
base_path = "/home/xyine/code_project/big_data_project/datalake"

tmdb_parquet_path = os.path.join(base_path, f"parquet/tmdb/NowPlaying/{current_day}")
tvmaze_parquet_path = os.path.join(base_path, f"parquet/tvmaze/new_series/{current_day}")
combined_parquet_path = os.path.join(base_path, f"combined/{current_day}")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Combine Parquet Files") \
    .getOrCreate()

# Function to combine Parquet files
def combine_parquet_files():
    # Read the TMDB and TVMaze Parquet files
    tmdb_df = spark.read.parquet(tmdb_parquet_path)
    tvmaze_df = spark.read.parquet(tvmaze_parquet_path)

    # Ensure schemas are the same for combining
    tvmaze_df = tvmaze_df.withColumnRenamed("language", "original_language") \
                         .withColumnRenamed("vote_count", "vote_count_tv") \
                         .withColumnRenamed("vote_average", "vote_average_tv")

    # Select common columns
    common_columns = ['title', 'release_date', 'overview', 'popularity', 'image', 'genres', 'id']
    tmdb_df = tmdb_df.select(common_columns + ['vote_average', 'vote_count', 'language'])
    tvmaze_df = tvmaze_df.select(common_columns + ['vote_average_tv', 'vote_count_tv', 'original_language'])

    # Union the DataFrames
    combined_df = tmdb_df.unionByName(tvmaze_df, allowMissingColumns=True)

    # Save the combined DataFrame as a Parquet file
    combined_df.write.mode("overwrite").parquet(combined_parquet_path)

    print(f"Combined data saved to {combined_parquet_path}")

# To be used in Airflow
def combine_parquet_task(**kwargs):
    combine_parquet_files()
    # Stop Spark session
    spark.stop()
