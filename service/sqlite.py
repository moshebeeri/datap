from data import Data
from projects.job import Job
import pymongo
import json 
from .service import Service
from rdbms import AlchemyEngineFactory
from sqlalchemy import Session
from sqlalchemy import text, bindparam
from sqlalchemy import Column, Integer, String, Table
import sqlite3

class SQLiteDB(Service):
  def __init__(self, db='source', table='default', connection='file::memory:?cache=shared', timestamp_name='timestamp'):
    self.collection = collection
    self.db = db
    self.connection_str = connection
    self.timestamp_name = timestamp_name
    self.table = table
    self.connect(connection)

  def connect(self):
    connection='file::memory:?cache=shared'
    pass

  def alchemy(self):
    self.engine = AlchemyEngineFactory(connection)
    self.connection = self.engine.connect()
    self.session = Session(bind=self.connection)
    # the best way is given by
    # https://docs.sqlalchemy.org/en/14/core/tutorial.html#using-textual-sql
    # The parameters can also be explicitly typed:
    # stmt = stmt.bindparams(bindparam("x", type_=String), bindparam("y", type_=String))

  def read(self, job: Job) -> Data:
    data = Data()
    with sqlite3.connect(self.connection_str) as conn:
      desc = text('.schema :table').bindparams(table=self.table)
      create_schema = conn.execute(desc)
      create_schema_stmt = job.set_create_schema(create_schema)
      data.add('create schema statement', create_schema_stmt)
      stmt = text("SELECT * FROM :t WHERE :tsn BETWEEN :from_time AND :to_time")
      stmt = stmt.bindparams(t=self.table, tsn=self.timestamp_name, from_time=job.from_time, to_time=job.to_time)
      result = conn.execute(stmt)
      for doc in result:
          data.add_doc(doc)
      conn.close()
    return data

  def write(self, data: Data, job: Job) -> Data:
    docs = data.get_docs()
    with sqlite3.connect(self.connection_str) as conn:
      c = conn.cursor()
      #records or rows in a list
      records = []
      for doc in docs:
        columns = ', '.join(doc.keys())
        values = ':' + ', :'.join(doc.values())
        query = 'INSERT INTO %s (%s) VALUES (%s)' % (self.table_name, columns, values)
        c.execute(query, doc)
      c.commit()
      conn.close()
    return data.set_docs(docs)