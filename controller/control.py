from collections import OrderedDict
from data import Data
from service import Service
from transform import Transform

class Control:
  conf = None
  source = None
  destination = None
  transforms = {}
  
  def __init__(self, conf={}):
    self.conf = conf

  def add_source(self, service: Service):
    self.source = service
    return self
  
  def add_destination(self, service: Service):
    self.destination = service
    return self

  def add_transform(self, pos, transform: Transform):
    self.transforms[pos] = transform
    return self

  def transform(self, data: Data) -> Data:
    od = OrderedDict(sorted(self.transforms.items()))
    ret = []
    for key, _ in od.items(): 
      print(key)
      ret.append(key)
    return ret
