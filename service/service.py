from abc import ABC, abstractmethod
from data.data import Data

class Service(ABC):

  def __init__(self, start, end, chunk_size=1024):
    super().__init__()

  @abstractmethod
  def connect(self, connection_string):
    pass

  @abstractmethod
  def read(self, address) -> Data:
    pass
  
  @abstractmethod
  def write(self, data: Data) -> Data:
    pass
