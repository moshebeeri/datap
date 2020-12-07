

import datetime
from dateutil import tz
from _datetime import timedelta
import json

class Projects():
  def __init__(self, mongodb_client, client_id):
    # mongodb.findbyId(project)
    self.client = mongodb_client
    self.database = self.client['operational']
    self.collection = self.database['projects']
    self.client_id = client_id
    self.activate_projects = self.get_projects(client_id)

  def get_projects(self, client_id, active_only=True):
    creteria = {'client_id': client_id, 'active': active_only} if active_only else {'client_id': client_id}
    projects = self.collection.find(creteria)
    return list(projects)

  def create_project(self, client_id, source, destination, days_to_keep=365, active=True):
    project = {'client_id': client_id, 
               'active': active,
               'days_to_keep': days_to_keep,
               'source':source,
               'destination': destination
              }
    _id = self.collection.insert_one(project).inserted_id
    return str(_id)

  def update_project_exec(self, project):
    now = datetime.now(tz.tzutc())
    today = datetime(
        now.year, now.month, now.day, minute=0, second=0, microsecond=0, tzinfo=tz.tzutc())
    self.collection.update_one({'_id': project['_id']},
                                  {'$set': {
                                      'ended': datetime.now(tz.tzutc()),
                                      'to': today - timedelta(days=project['days_to_keep']),
                                      'from': project['to']
                                  }})

  def halt_project(self, project_id):
    self.collection.update_one({'_id': project_id},
                                  {'$set': {
                                      'status': 'halt',
                                      'active': False
                                  }})

  def activate_project(self, project_id):
    self.collection.update_one({'_id': project_id},
                                  {'$set': {
                                      'status': 'active',
                                      'active': True
                                  }})
  def get_next_jobs(self, project_id):
    pass

  def get_last_jobs(self, project_id):
    pass
