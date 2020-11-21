from .project import Project

class Job():
  def __init__(self, project: Project):
    self.project = project
  
  def __init__(self):
    self.project = project
  
  def getProject(self) -> Project:
    return self.project