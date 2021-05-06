
from controller import Control
import mongomock
from elasticmock import elasticmock
from datetime import datetime
from freezegun import freeze_time
from datetime import datetime
from unittest import TestCase
from service import *
from projects.job import Job
import random
from sqlite3.dbapi2 import Error

class SQLiteToMongoTest(TestCase):
  sqlite_engine = None
  sql_create_projects_table = """ CREATE TABLE IF NOT EXISTS projects (
                                      id integer PRIMARY KEY,
                                      name text NOT NULL,
                                      timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                  ); """

  def create_table(self, conn):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(self.sql_create_projects_table)
    except Error as e:
        print(e)

  def sqlite_populate_many(self, sqlite):
    objects = []
    conn = sqlite.conn
    self.create_table(conn)
    c = conn.cursor()
    for i in range(100):
      insert_stm = "INSERT INTO projects (name) VALUES('name_" + str(i) + "')"
      c.execute(insert_stm)
    c.commit()

  @mongomock.patch(servers=(('example.com', 27017),))
  def test_transfer_elastic_to_mongo(self):
    control = Control({'retryable': False})
    sqlite = SQLiteDB()
    self.sqlite_populate_many(sqlite)
    mongodb = MongoDB(connection = 'example.com')
    control = control.add_source(sqlite).add_destination(mongodb)
    assert control != None
    docs = control.run(Job(from_time=datetime(2020, 11, 11), to_time= datetime(2020, 11, 12)))
    assert len(docs) == 500
