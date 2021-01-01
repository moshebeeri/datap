#https://stackoverflow.com/questions/21770829/sqlalchemy-copy-schema-and-data-of-subquery-to-another-database


from sqlalchemy import create_engine, Table, Column, Integer, Unicode, MetaData, String, Text, update, and_, select, func, types

#see https://gist.github.com/pawl/9935333
class AlchemyTableClone:
  ###
  # Source and Destination should be of the following form:
  #{
  #   engine: 'SQLAlchemy Engine'
  #   table: 'the_table_name'
  # }
  def clone(self, source, destination):
    # create engine, reflect existing columns, and create table object for oldTable
    srcEngine = source['engine']
    srcEngine._metadata = MetaData(bind=srcEngine)
    srcEngine._metadata.reflect(srcEngine) # get columns from existing table
    srcTable = Table(source['table'], srcEngine._metadata)

    # create engine and table object for newTable
    destEngine = destination['engine']
    destEngine._metadata = MetaData(bind=destEngine)
    destTable = Table(destination['table'], destEngine._metadata)

    # copy schema and create newTable from oldTable
    for column in srcTable.columns:
        destTable.append_column(column.copy())
    destTable.create()
