from .service import Service
from data import Data

class Elasticsearch(Service):
  def __init__(self):
    pass

  def connect(self, connection_string):
    pass

  def read(self, address) -> Data:
    pass
  
  def write(self, data: Data) -> Data:
    pass