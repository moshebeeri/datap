from service import Elasticsearch
from controller import Control
import mongomock
from elasticmock import elasticmock
from datetime import datetime
from freezegun import freeze_time
from datetime import datetime
from service.mongodb import MongoDB

class TestControlBuilders:

  @mongomock.patch(servers=(('example.com', 27017),))
  def test_control_builders(self):
    control = Control({'retryable': False})
    builder = control.add_source(MongoDB(connection = 'example.com')).add_destination(Elasticsearch())
    assert builder != None
    builder.add_transform('2', None)
    builder.add_transform('1', None)
    builder.add_transform('3', None)
    order = builder.transform(None)
    assert order == ['1', '2', '3']
