import os
import boto3
#https://stackoverflow.com/questions/21770829/sqlalchemy-copy-schema-and-data-of-subquery-to-another-database
from sqlalchemy import create_engine, MetaData
from sqlalchemy import Column, Integer, String, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from sqlalchemy import bindparam


class AlchemyEngineFactory:
  def sqlite_inmemory(self):
    return self.sqlite('/:memory:')

  def sqlite(self, file_path_name):
    return create_engine('sqlite://' + file_path_name, echo=True)

  def mysql(self, username, password, url, database):
    return create_engine('mysql+mysqldb://'+username+':'+password+'@'+url+'/'+database) 


  # https://stackoverflow.com/questions/46328762/connecting-to-amazon-aurora-using-sqlalchemy
  def rds(self, protocol, user, host, port, database):
    rds_host = host     # or os.environ.get("RDS_HOST")  
    rds_port = port     # or os.environ.get("RDS_PORT")
    rds_username = user # or os.environ.get("RDS_USER")   

    temp_passwd = boto3.client('rds').generate_db_auth_token(
        DBHostname=rds_host,
        Port=rds_port,
        DBUsername=rds_username 
    )

    rds_credentials = {'user': rds_username, 'passwd': temp_passwd}

    conn_str = f"{protocol}://{rds_host}:{rds_port}/{database}"
    kw = dict()
    kw.update(rds_credentials)
    # kw.update({'ssl': {'ca': /path/to/pem-file.pem}})  # MySQL
    # kw.update({'sslmode': 'verify-full', 'sslrootcert /path/to/pem-file.pem})  # PostgreSQL

    return  create_engine(conn_str, connect_args=kw)
    # session = sessionmaker(engine)
    # return session

  
  def query_example(self):
    from sqlalchemy import create_engine, select, MetaData, Table

    engine = create_engine("dburl://user:pass@database/schema")
    metadata = MetaData(bind=None)
    table = Table('table_name', metadata, autoload = True, autoload_with = engine)
    stmt = select([table]).where(table.columns.column_name == 'filter')

    connection = engine.connect()
    # results = con.execute('SELECT * FROM book')
    results = connection.execute(stmt).fetchall()
    for result in results:
        print(result)
    
    with engine.connect() as connection:
      result = connection.execute(text("select username from users"))
      for row in result:
          print("username:", row['username'])

      # the best way is given by
      # https://docs.sqlalchemy.org/en/14/core/tutorial.html#using-textual-sql
      stmt = text("SELECT * FROM users WHERE users.name BETWEEN :x AND :y")
      stmt = stmt.bindparams(x="m", y="z")
      # The parameters can also be explicitly typed:

      stmt = stmt.bindparams(bindparam("x", type_=String), bindparam("y", type_=String))
      result = connection.execute(stmt, {"x": "m", "y": "z"})


