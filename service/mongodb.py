
from .service import Service
from data import Data
from projects import Job
import pymongo

class MongoDB(Service):
  def __init__(self, db='source', collection='default', connection = 'mongodb://localhost:27017/'):
    self.collection = collection
    self.connect(connection)
    self.db = db

  def creteria(self, jon: Job):
    return {}

  def connect(self, connection = 'mongodb://localhost:27017/'):
    self.client = pymongo.MongoClient(connection)

  def read(self, job: Job) -> Data:
    data = Data()
    creteria = self.creteria(job)
    docs = self.client[self.db][self.collection].find(creteria)
    for doc in docs:
      doc_data = json.loads(json.dumps(doc, sort_keys=True, default=str))
      data.add_doc(doc_data)
    return data

  def write(self, data: Data, job: Job) -> Data:
    pass
