#https://stackoverflow.com/questions/21770829/sqlalchemy-copy-schema-and-data-of-subquery-to-another-database


from sqlalchemy import create_engine, Table, Column, Integer, Unicode, MetaData, String, Text, update, and_, select, func, types

#see https://gist.github.com/pawl/9935333
class AlchemyTableClone:
  def clone(self):
    # create engine, reflect existing columns, and create table object for oldTable
    srcEngine = create_engine('mysql+mysqldb://username:password@111.111.111.111/database') # change this for your source database
    srcEngine._metadata = MetaData(bind=srcEngine)
    srcEngine._metadata.reflect(srcEngine) # get columns from existing table
    srcTable = Table('oldTable', srcEngine._metadata)

    # create engine and table object for newTable
    destEngine = create_engine('mysql+mysqldb://username:password@localhost/database') # change this for your destination database
    destEngine._metadata = MetaData(bind=destEngine)
    destTable = Table('newTable', destEngine._metadata)

    # copy schema and create newTable from oldTable
    for column in srcTable.columns:
        destTable.append_column(column.copy())
    destTable.create()
