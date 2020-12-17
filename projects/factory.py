

from service.mongodb import MongoDB
from elasticsearch.client import Elasticsearch

class ServiceFactory:
  def create_service(self, project_config):
    type_ = project_config['type']
    if type_ == 'mongodb':
      print('mongodb service created')
      mongodb = MongoDB(connection=project_config['connection'])
      return mongodb

    if type_ == 'elastic':
      print('elastic service created')
      elastic = Elasticsearch(index=project_config.index, connection=project_config.connection)
      return elastic

    if type_ == 'sqlite':
      print('sqlite service created')
