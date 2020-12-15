from service import MongoDB
from service import Elasticsearch
from controller import Control
import mongomock
from elasticmock import elasticmock
from datetime import datetime
from freezegun import freeze_time
from datetime import datetime
import elasticsearch
import unittest


class TestElasticToMongo(unittest.TestCase):

  def mongo_populate_many(self, json_store):
    objects = []
    for i in range(100):
      obj = {
        'some_field' : i,
        'timestamp':  datetime.now()
      }
      objects.append(obj)  
    return json_store.insert_many(objects)

  @elasticmock
  @freeze_time("2020-11-11 00:00:00")
  def elastic_populate_many(self):
    index = 'my_data'
    
    # Instantiate service
    service = Elasticsearch()
    freezer = freeze_time("2020-11-11 00:00:00")
    freezer.start()
    for i in range(500):
      body = {
          'i': i,
          'timestamp': datetime.now()
      }
      es_object = self.es.index(index, body)

  @mongomock.patch(servers=(('example.com', 27017),))
  @elasticmock
  def test_transfer_elastic_to_mongo(self):
    control = Control({'retryable': False})
    elastic = Elasticsearch()
    self.elastic_populate_many(elastic)
    mongodb = MongoDB(connection = 'example.com')
    control = control.add_source(elastic).add_destination(mongodb)
    assert control != None
    data = control.run()
    docs = data.get_docs()
    assert len(docs) == 100
