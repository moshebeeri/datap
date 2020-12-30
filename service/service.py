from abc import ABC, abstractmethod
from data.data import Data
from projects.job import Job

class Service(ABC):
  
  def __init__(self, start, end, type='nosql', chunk_size=1024):
    super().__init__()

  @abstractmethod
  def connect(self, connection):
    pass

  @abstractmethod
  def read(self, job: Job) -> Data:
    pass
  
  @abstractmethod
  def write(self, data: Data, job: Job) -> Data:
    pass
