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

class TestControl(unittest.TestCase):

  def populate_many(self, json_store):
    objects = []
    for i in range(100):
      obj = {
        'some_field' : i,
        'timestamp':  datetime.now()
      }
      objects.append(obj)  
    return json_store.insert_many(objects)

  def populate_json_store(self, json_store):
    objects = []
    for i in range(500):
      obj = dict(votes=i)
      obj['_id'] = json_store.insert_one(obj).inserted_id
      objects.append(obj)
    return list(json_store.find({}))

  def test_mongomock(self):
    client = mongomock.MongoClient('mongodb.server.example.com')
    json_store = client['source']['json_store']
    objects = self.populate_json_store(json_store)
    assert len(objects) == 500

  @elasticmock  
  def test_elasticmock(self):
    index = 'test-index'
    expected_document = {
        'foo': 'bar'
    }
    # Instantiate service
    service = Elasticsearch()
    # Index document on ElasticSearch
    id = service.create(index, expected_document)
    self.assertIsNotNone(id)
    # Retrive document from ElasticSearch
    document = service.read(index, id)
    self.assertEqual(expected_document, document)

  @mongomock.patch(servers=(('example.com', 27017),))
  @elasticmock
  def test_transfer_mongo_to_elastic_(self):
    control = Control({'retryable': False})
    mongodb = MongoDB(connection = 'example.com')
    #mongodb.db_collection().insert_many([dict(votes=1), dict(votes=2)])
    self.populate_many(mongodb.db_collection())

    control = control.add_source(mongodb).add_destination(Elasticsearch())
    assert control != None
    data = control.run()
    docs = data.get_docs()
    assert len(docs) == 100
