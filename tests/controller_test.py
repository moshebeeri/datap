from service import MongoDB
from service import Elasticsearch
from controller import Control

class TestControl:
  def source(self):
    return MongoDB()
 
  def destination(self):
    return Elasticsearch()

  def test_control_builders(self):
    control = Control({'retryable': False})
    builder = control.add_source(self.source()).add_destination(self.destination())
    assert builder != None
    builder.add_transform('2', None)
    builder.add_transform('1', None)
    builder.add_transform('3', None)
    order = builder.transform(None)
    assert order == ['1', '2', '3']




    