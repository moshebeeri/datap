from data import Data
import elasticsearch
from projects.job import Job
from .service import *

class Elasticsearch(Service):
  
  def __init__(self, index='default', connection={'hosts': [{'host': 'localhost', 'port': 9200}]}, timestamp_name='timestamp'):
    self.index = index
    self.timestamp_name = timestamp_name
    self.connect(connection)

  def connect(self, connection={'hosts': [{'host': 'localhost', 'port': 9200}]}):
    self.es = elasticsearch.Elasticsearch(hosts=connection['hosts'])

  def read(self, job: Job) -> Data:
    data = Data()
    timestamp = {
      "gte": str(job.from_time),
      "lt": str(job.to_time)
    }
    body={
      "query": {
        "range": {
        }
      }
    }
    body["query"]["range"][self.timestamp_name] = timestamp
    res = self.es.search(index=self.index, body=body)
    hits = res['hits']['hits']
    for doc in res['hits']['hits']:
      data.add_doc(doc)
    return data

  def write(self, data: Data, job: Job) -> Data:
    docs = data.get_docs()
    for doc in docs:
      es_object = self.es.index(self.index, doc)
      doc['es_id'] = es_object.get('_id')
    return data.set_docs(docs)