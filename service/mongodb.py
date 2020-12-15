from .service import Service
from data import Data
from projects import Job
import pymongo
import json

class MongoDB(Service):
  def __init__(self, db='source', collection='default', connection = 'mongodb://localhost:27017/'):
    self.collection = collection
    self.db = db
    self.connection = connection
    self.connect(connection)

  def creteria(self, jon: Job):
    return {}

  def connect(self, connection = 'mongodb://localhost:27017/'):
    self.client = pymongo.MongoClient(connection)

  def db_collection(self):
    return self.client[self.db][self.collection]

  def read(self, job: Job) -> Data:
    data = Data()
    creteria = self.creteria(job)
    col = self.db_collection()
    docs = col.find(creteria)
    for doc in docs:
      doc_data = json.loads(json.dumps(doc, sort_keys=True, default=str))
      data.add_doc(doc_data)
    return data

  def write(self, data: Data, job: Job) -> Data:
    docs = data.get_docs()
    result = self.collection.insert_many(docs)
    in_ids = result.inserted_ids
    for i in range(len(in_ids)):
      docs[i]['_id'] = in_ids[i]
    return data.set_docs(docs)