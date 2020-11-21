from .service import Service
from data import Data
import elasticsearch
from projects import Job

class Elasticsearch(Service):
  
  def __init__(self, index='default'):
    self.index = index

  def connect(self, connection=[{'host': 'localhost', 'port': 9200}]):
    self.es = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])

  def read(self, job: Job) -> Data:
    pass
  
  def write(self, data: Data, job: Job) -> Data:
    docs = data.get_docs()
    data.clear()
    for doc in docs:
      es_object = self.es.index(self.index, doc)
      doc['es_id'] = es_object.get('_id')
      data.add_doc(doc)
    return data