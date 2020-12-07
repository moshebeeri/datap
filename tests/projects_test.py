import pytest
from service import MongoDB
from projects import Projects
import mongomock
from elasticmock import elasticmock
from datetime import datetime
from freezegun import freeze_time
from datetime import datetime
import pymongo

class TestProjects:

  @pytest.fixture(scope="session", autouse=True)
  @mongomock.patch(servers=(('example.com', 27017),))
  def mongodb_client(self):
    return pymongo.MongoClient('example.com')
    
  @mongomock.patch(servers=(('example.com', 27017),))
  def test_projects(self, mongodb_client):
    clientId = "client1"
    projects = Projects(mongodb_client, clientId)
    project_id = projects.create_project(clientId, {
      'type': 'mongodb',
      'host': 'example.com',
      'port': 27017,
      'user': 'admin',
      'password': 'password'
    }, {
      'type': 'elasticsearch',
      'host': 'elastic.com',
      'user': 'admin',
      'password': 'password'
    })
    assert project_id
    returns = projects.get_projects(clientId)
    assert len(returns) == 1


  @mongomock.patch(servers=(('example.com', 27017),))
  def test_get_projects(self, mongodb_client):
    clientId = "client1"
    projects = Projects(mongodb_client, clientId)
    active = projects.get_projects(clientId)
    assert len(active) == 1


  @mongomock.patch(servers=(('example.com', 27017),))
  @elasticmock
  def test_transfer_with_project(self):
    pass


  
