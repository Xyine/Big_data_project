# index_to_elasticsearch.py
import os
from datetime import date
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch

# Initialize Elasticsearch client
es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "SZGn=7v9b99DtbQal4gM"),
    verify_certs=False
)

# Define paths
current_day = date.today().strftime("%Y%m%d")
base_path = "/home/xyine/code_project/big_data_project/datalake"

combined_parquet_path = os.path.join(base_path, f"combined/{current_day}")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Index to Elasticsearch") \
    .getOrCreate()

# Read the combined Parquet file
combined_df = spark.read.parquet(combined_parquet_path)

# Convert Spark DataFrame to list of dictionaries
actions = combined_df.toPandas().to_dict(orient='records')

# Function to index each document into Elasticsearch
def index_documents(actions, index_name="movies_index"):
    for action in actions:
        es.index(index=index_name, body=action)

# To be used in Airflow
def index_to_elasticsearch_task(**kwargs):
    index_documents(actions)
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    index_to_elasticsearch_task()
