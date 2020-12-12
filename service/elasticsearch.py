from .service import Service
from data import Data
import elasticsearch
from projects import Job

class Elasticsearch(Service):
  
  def __init__(self, index='default', connection=[{'host': 'localhost', 'port': 9200}]):
    self.index = index
    self.connect()

  def connect(self, hosts=[{'host': 'localhost', 'port': 9200}]):
    self.es = elasticsearch.Elasticsearch(hosts=hosts)

  def read(self, job: Job) -> Data:
    data = Data()
    res = es.search(index="test-index", body={
      "query": {
        #"match_all": {}
        "range": {
          "timestamp": {
            "gte": job.from_time,
            "lt": job.to_time
          }
        }
      }
    })
    hits = res['hits']['hits']
    # for num, doc in enumerate(hits):
    #   data.add_doc(doc)
    for doc in res['hits']['hits']:
      data.add_doc(doc)
      #print("%s) %s" % (doc['_id'], doc['_source']['content']))

    return data

  def write(self, data: Data, job: Job) -> Data:
    docs = data.get_docs()
    for doc in docs:
      es_object = self.es.index(self.index, doc)
      doc['es_id'] = es_object.get('_id')
    return data.set_docs(docs)