
from controller import Control
import mongomock
from elasticmock import elasticmock
from datetime import datetime
from freezegun import freeze_time
from datetime import datetime
import elasticsearch
from unittest import TestCase
from service import *
from projects.job import Job
import random
from rdbms import AlchemyEngineFactory
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
class TestData(Base):
  __tablename__ = 'TestData'

  id = Column(Integer, primary_key=True)
  number = Column(Integer)  
  timestamp = Column(DateTime, default=datetime.datetime.utcnow)

  def __repr__(self):
     return "<User(number='%d', timestamp='%s')>" % (self.number, str(self.timestamp))

class TestSQLiteToMongo(TestCase):
  engineFactory: AlchemyEngineFactory = None
  sqlite_engine = None

  @classmethod
  def setUpClass(cls):
    super(TestSQLiteToMongo, cls).setUpClass()
    cls.engineFactory = AlchemyEngineFactory()
    cls.sqlite_engine = cls.engineFactory.sqlite_in_memory()

  def sqlite_populate_many(self, session):
    objects = []
    for i in range(100):
      test_data = TestData(number=i + 100)
      session.add(test_data)
      session.commit()

  @mongomock.patch(servers=(('example.com', 27017),))
  def test_transfer_elastic_to_mongo(self):
    control = Control({'retryable': False})
    sqlite = SQLite()
    # elastic = Elasticsearch(index='my_data')
    # self.elastic_populate_many(elastic)
    mongodb = MongoDB(connection = 'example.com')
    control = control.add_source(sqlite).add_destination(mongodb)
    assert control != None
    docs = control.run(Job(from_time=datetime(2020, 11, 11), to_time= datetime(2020, 11, 12)))
    assert len(docs) == 500
