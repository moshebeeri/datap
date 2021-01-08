from service.service import Service
from projects.job import Job
from data.data import Data

class Aurora(Service):
  def connect(self, connection):
    pass

  def read(self, job: Job) -> Data:
    pass
  
  def write(self, data: Data, job: Job) -> Data:
    pass
