from data import Data
from projects.job import Job
import pymongo
import json 
from .service import Service
import sqlite3

class SQLiteDB(Service):
  def __init__(self, db='source', table='default', connection='file::memory:?cache=shared', timestamp_name='timestamp'):
    self.db = db
    self.connection_str = connection
    self.timestamp_name = timestamp_name
    self.table = table
    self.connect()

  def connect(self):
    self.conn = sqlite3.connect(self.connection_str)

  def read(self, job: Job) -> Data:
    data = Data()
    # desc = '.schema ' + self.table
    # create_schema = self.conn.execute(desc)
    # create_schema_stmt = job.set_create_schema(create_schema)
    # data.add('create schema statement', create_schema_stmt)
    # stmt = 'SELECT * FROM {} WHERE {} BETWEEN "{}" AND "{}"'.format(self.table, self.timestamp_name, job.from_time, job.to_time)
    stmt = 'SELECT * FROM {}'.format(self.table)
    result = self.conn.execute(stmt)
    for doc in result:
        data.add_doc(doc)
    return data

  def write(self, data: Data, job: Job) -> Data:
    docs = data.get_docs()
    with conn.cursor() as c:
      #records or rows in a list
      records = []
      for doc in docs:
        columns = ', '.join(doc.keys())
        values = ':' + ', :'.join(doc.values())
        query = 'INSERT INTO %s (%s) VALUES (%s)' % (self.table_name, columns, values)
        c.execute(query, doc)
      c.commit()
    return data.set_docs(docs)