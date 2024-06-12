import os
from pyspark.sql import SparkSession

# Set environment variables for PySpark
# os.environ['PYSPARK_PYTHON'] = r'C:\Users\desag\AppData\Local\Programs\Python\Python312\python.exe'
# os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\desag\AppData\Local\Programs\Python\Python312\python.exe'

# Initialize Spark session
spark = SparkSession.builder.appName("IndexingData").getOrCreate()

# Path to your combined Parquet file
combined_parquet_path = "C:/Users/desag/OneDrive/Documents/A2/advance_database_and_big_data/big_data_project/datalake/combined/20240611"

# Load the Parquet file into a DataFrame
combined_df = spark.read.parquet(combined_parquet_path)

# Function to transform each row to a dictionary for Elasticsearch
def transform_to_dict(row):
    return row.asDict()

action = combined_df.rdd.map(transform_to_dict).collect()
print(action)
# Collect data and transform to list of dictionaries
#actions = combined_df.rdd.map(transform_to_dict).collect()

# Stop Spark session
spark.stop()

# Return list of dictionaries
#actions
