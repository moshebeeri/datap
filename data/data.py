class Data():
  docs = []

  def __init__(self, docs=[]):
    self.docs = docs

  def add_doc(self, doc):
    self.docs.append(doc)

  def set_docs(self, docs=[]):
    self.docs = docs
    return self.docs

  def get_docs(self):
    return self.docs
