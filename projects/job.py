from .projects import Projects

class Job():
  def __init__(self, projects: Projects):
    self.projects = projects
  
  def __init__(self):
    self.projects = projects
  
  def getProjects(self) -> Projects:
    return self.projects