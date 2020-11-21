class Data():
  docs = []
  def __init__(self):
    pass

  def add_doc(self, doc):
    self.docs.append(doc)

  def get_docs(self):
    return self.docs

  def clear(self):
    self.docs = []
