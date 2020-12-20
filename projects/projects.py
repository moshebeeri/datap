from datetime import datetime
from dateutil import tz
from _datetime import timedelta
import json
from .job import Job
from .factory import ServiceFactory
# from service.mongodb import MongoDB
# from service.elasticsearch import Elasticsearch

class Projects():
  def __init__(self, firestore_client, user_id):
    self.db = firestore_client
    self.projects = self.db.collection(u'projects')
    self.user_id = user_id
    self.activate_projects = self.get_projects(user_id)

  def get_projects(self, user_id, active_only=True):
    active = [True] if active_only else [True, False]
    return self.projects.where('user_id', '==', user_id).where('active', 'in', active).stream()

  def create_services(self, project):
    sf = ServiceFactory()
    self.source = sf.create_service(project['source'])
    self.destination = sf.create_service(project['destination'])

  def create_project(self, user_id, source, destination, start, interval=24*60*60, seconds_to_keep=365*24*60*60, active=True):
    project = {'user_id': user_id, 
               'active': active,
               'start': start,
               'interval': interval,
               'seconds_to_keep': seconds_to_keep,
               'source':source,
               'destination': destination
              }
    ref = self.projects.add(project)
    self.create_services(project)
    return ref[1].id


  def update_project_exec(self, project, job):
    data = project.to_dict()
    now = datetime.now(tz.tzutc())
    proj_ref = self.projects.document(project.id)
    proj_ref.update({
      'ended': datetime.now(tz.tzutc()),
      'to': job.to_time + timedelta(seconds=data['interval']),
      'from': job.to_time
      })

  def halt_project(self, project_id):
    proj_ref = self.projects.document(project_id)
    proj_ref.update({
      'status': 'halt',
      'active': False
      })

  def activate_project(self, project_id):
    proj_ref = self.projects.document(project_id)
    proj_ref.update({
      'status': 'active',
      'active': True
      })
 
  def get_job(self, project):
    return Job(project)
  
  def get_last_job(self, project):
    if 'last_job' in project:
      return project['last_job']
    return None

  def get_control(self, project):
    pass