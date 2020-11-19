from service import MongoDB
from service import Elasticsearch
from controller import Control
import mongomock
from elasticmock import elasticmock
from datetime import datetime
from freezegun import freeze_time
from datetime import datetime

class TestControl:

  def populate(json_store):
    objects = []
    for i in range(100):
      objects.append({
        'some_field' : i,
        'timestamp': datetime.now()
      })
    json_store.insert_many(objects)

  def populate_json_store(self, json_store):
    objects = []
    for i in range(500):
      obj = dict(votes=i)
      obj['_id'] = json_store.insert_one(obj).inserted_id
      objects.append(obj)
    return objects

  @mongomock.patch(servers=(('mongodb.server.example.com', 27017),))
  def test_control_builders(self):
    control = Control({'retryable': False})
    client = mongomock.MongoClient('server.example.com')
    json_store = client['source']['json_store']
    objects = self.populate_json_store(json_store)
    assert len(objects) == 500

    builder = control.add_source(MongoDB()).add_destination(Elasticsearch())
    assert builder != None
    builder.add_transform('2', None)
    builder.add_transform('1', None)
    builder.add_transform('3', None)
    order = builder.transform(None)
    assert order == ['1', '2', '3']


  