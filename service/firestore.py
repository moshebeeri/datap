from data import Data
from projects.job import Job
import json 
from .service import Service

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore


class Firestore(Service):
  def __init__(self, service_account_path_file, timestamp_name='timestamp', collection='default'):

    cred = credentials.Certificate(service_account_path_file)
    firebase_admin.initialize_app(cred)
    self.db = firestore.client()
    self.collection = self.db.collection(collection)
    self.timestamp_name = timestamp_name

    def connect(self, connection={}):
      pass
    
    def db_collection(self):
      return self.client[self.db][self.collection]

  def read(self, job: Job) -> Data:
    data = Data()
    docs = self.collection.where(timestamp_name, u'>=', job.from_time).where(timestamp_name, u'<', job.to_time).stream()
    for doc in docs:
      data.add_doc(doc.to_dict())
    return data

  def write(self, data: Data, job: Job) -> Data:
    docs = data.get_docs()
    batch = db.batch()
    for doc in docs:
      self.collection.set(doc)
    batch.commit()
    return data.set_docs(docs)
