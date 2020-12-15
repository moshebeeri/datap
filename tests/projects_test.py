import pytest
from service import MongoDB
from projects import Projects
import mongomock
from elasticmock import elasticmock
from datetime import datetime
from freezegun import freeze_time
from datetime import datetime
from datetime import timedelta
import pymongo
from mockfirestore import MockFirestore

class TestProjects:

  @pytest.fixture(scope="session", autouse=True)
  @mongomock.patch(servers=(('example.com', 27017),))
  def mongodb_client(self):
    return pymongo.MongoClient('example.com')
  
  @pytest.fixture(scope="session", autouse=True)
  def firestore(self):
    return MockFirestore()

  @freeze_time("2020-11-11 00:00:00")
  @mongomock.patch(servers=(('example.com', 27017),))
  def test_projects(self, firestore):
    userId = "user1"
    projects = Projects(firestore, userId)
    start = datetime.now() - timedelta(days=3)
    ref = projects.create_project(userId, {
      'type': 'mongodb',
      'host': 'example.com',
      'port': 27017,
      'user': 'admin',
      'password': 'password'
      }, {
        'type': 'elasticsearch',
        'host': 'elastic.com',
        'port': 9200,
        'user': 'admin',
        'password': 'password'
      },
      start=start,
      seconds_to_keep=12 * 60 * 60
    )
    assert ref
    returns = projects.get_projects(userId)
    assert len(list(returns)) == 1

  def test_get_projects(self, firestore):
    userId = "user1"
    projects = Projects(firestore, userId)
    active = projects.get_projects(userId)
    assert len(list(active)) == 1

  @freeze_time("2020-11-11 00:00:00")
  def test_time_freeze(self):
    freezer = freeze_time("2020-11-12 00:00:00")
    freezer.start()
    assert datetime(2020, 11, 12) == datetime.now()

  @freeze_time("2020-11-11 00:00:00")
  def test_exec_all(self, firestore):
    userId = "user1"
    projects = Projects(firestore, userId)
    active_projects = projects.get_projects(userId)
    for project in active_projects:
      job = projects.get_job(project.to_dict())
      assert job.from_time == datetime(2020,11,8,0,0,0)
      assert job.to_time == datetime(2020, 11, 9, 0, 0, 0)
      projects.update_project_exec(project, job)
   
    active_projects = projects.get_projects(userId)
    for project in active_projects:
      job = projects.get_job(project.to_dict())
      assert job.from_time == datetime(2020,11,9,0,0,0)
      assert job.to_time == datetime(2020, 11, 10, 0, 0, 0)
      projects.update_project_exec(project, job)

    active_projects = projects.get_projects(userId)
    for project in active_projects:
      job = projects.get_job(project.to_dict())
      assert job.from_time == datetime(2020,11,10,0,0,0)
      assert job.to_time == datetime(2020, 11, 10, 12, 0, 0)
      projects.update_project_exec(project, job)

    freeze_time("2020-11-12 00:00:00").start()
    active_projects = projects.get_projects(userId)
    for project in active_projects:
      job = projects.get_job(project.to_dict())
      assert job.from_time == datetime(2020, 11, 10, 12, 0, 0)
      assert job.to_time == datetime(2020, 11, 11, 12, 0, 0)
      projects.update_project_exec(project, job)


  
