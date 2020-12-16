from datetime import timedelta
from datetime import datetime

class Job():
  from_time= datetime.now()
  to_time = datetime.now()

  def __init__(self, project=None, from_time=None, to_time=None):
    if project is not None:
      self.from_time = project['from'] if 'ended' in project else project['start']
      self.to_time = self.from_time + timedelta(seconds=project['interval'])
      # check keep time is valid
      now = datetime.now()
      threshold = now - timedelta(seconds=project['seconds_to_keep'])
      if self.to_time > threshold:
        self.to_time = threshold
      if self.from_time > threshold:
        self.from_time = threshold
    
    if from_time is not None and to_time is not None and from_time <= to_time:
      self.from_time = from_time
      self.to_time = to_time
