from elasticsearch import Elasticsearch

# Initialize Elasticsearch client
es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "SZGn=7v9b99DtbQal4gM"),
    verify_certs=False
)

# Function to index each document into Elasticsearch
def index_documents(actions, index_name="movies_index"):
    for action in actions:
        es.index(index=index_name, body=action)

# Call function to index documents
index_documents(actions)
