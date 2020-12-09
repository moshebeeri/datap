from datetime import timedelta
from datetime import datetime

class Job():
  
  def __init__(self, project):
    self.from_time = project['from'] if 'ended' in project else project['start']
    self.to_time = self.from_time + timedelta(seconds=project['interval'])
    # check keep time is valid
    now = datetime.now()
    threshold = now - timedelta(seconds=project['seconds_to_keep'])
    self.valid =  self.to_time <= threshold
