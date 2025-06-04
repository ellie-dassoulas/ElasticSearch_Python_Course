from tqdm import tqdm
import json
from pprint import pprint
from typing import List
from config import INDEX_NAME
from utils import get_es_client
from elasticsearch import Elasticsearch

def index_data(documents: List[dict]):
    es = get_es_client(max_retries = 5, sleep_time=5)
    _ = _create_index(es=es, index_name=INDEX_NAME)
    _ = _insert_documents(es=es, index_name=INDEX_NAME, documents=documents)
    pprint(f'Indexed {len(documents)} documents into Elasticsearch index "{INDEX_NAME}"') #prints how many documents were indexed

def _create_index(es: Elasticsearch) -> dict:
    es.indices.delete(index=INDEX_NAME, ignore_unavailable=True)
    return es.indices.create(index=INDEX_NAME)

#getting list of documents, es client... making operations and passing it to bulk API
def _insert_documents(es: Elasticsearch, documents: List[dict]) -> dict:
    operations = []
    for document in tqdm(documents, total=len(documents), dec='Indexing documents'):
        operations.append({'index': {'_index': INDEX_NAME}})
        operations.append(document)
    return es.bulk(operations=operations)

if __name__ == '__main__':
    with open('../../../data/apod.json') as f:
        documents = json.load(f)    #kept the list of documents, passed to the index_data method

    index_data(documents=documents)

