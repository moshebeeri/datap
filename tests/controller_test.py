from service.mongodb import MongoDB
from service.elasticsearch import Elasticsearch
from controller import Control

class TestControl:
  def source(self):
    return MongoDB()
 
  def destination(self):
    return Elasticsearch()

  def test_control_builders(self):
    control = Control({'retryable': False})




    