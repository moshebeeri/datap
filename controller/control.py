from collections import OrderedDict
from data import Data
from service import Service
from transform import Transform

class Control:
  conf = None
  source = None
  destination = None
  transforms = OrderedDict()
  
  def __init__(self, conf={}):
    self.conf = conf

  def add_source(self, service: Service):
    self.source = service
    return self
  
  def add_destination(self, service: Service):
    self.destination = service
    return self

  def add_transform(self, pos, transform: Transform):
    transforms[pos] = transform
    return self

  def transform(self): Data