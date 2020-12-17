from service.druid import Druid

class TestImport:

  def test_it(self):
    druid = Druid(start=None, end=None)
    assert True