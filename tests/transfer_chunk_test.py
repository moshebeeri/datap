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


class ElasticService:

    def __init__(self):
        self.es = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])

    def create(self, index, body):
        es_object = self.es.index(index, body)
        return es_object.get('_id')

    def read(self, index, id):
        es_object = self.es.get(index, id)
        return es_object.get('_source')


class TestControl(unittest.TestCase):

  def populateMany(json_store):
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
    service = ElasticService()
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
    mongodb = MongoDB(connection = 'mongodb://example.com:27017/')
    control = control.add_source(mongodb).add_destination(Elasticsearch())
    assert control != None
    data = control.run()
    assert data != None
