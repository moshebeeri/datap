import json
from data import Data
from google.cloud import storage
from .service import Service

class GCloudStorage(Service):

  def __init__(self, region='US-EAST1', bucket='backup', path='/'):
    self.region = region
    self.bucket = bucket
    self.path = path

  def connect(self, connection={}):
    self.storage_client = storage.Client()
    self.bucket = self.storage_client.bucket(self.bucket)

  def read(self, job: Job) -> Data:
    data = Data()
    # get bucket data as blob
    blob = self.bucket.get_blob(self.path+job.from_time+'.jsonl')
    # convert to string
    jsonl_data = blob.download_as_string()
    for line in jsonl_data.splitlines():
      doc_data = json.loads(line)
      data.add_doc(doc_data)
    return data

  def write(self, data: Data, job: Job) -> Data:
    docs = data.get_docs()
    content=[]
    for doc in docs:
      l = json.dumps(doc, sort_keys=True, default=str)
      content.append(l)
    blob = self.bucket.blob(self.path+job.from_time+'.jsonl')
    blob.upload_from_string('\n'.join(content))
    return data

