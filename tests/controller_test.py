from mock import patch, Mock
import pytest
from pytest_mock import mocker
from message.ping import Message
from datetime import datetime
from freezegun import freeze_time
import json

class TestPingMessage:
  def create_message(self):
    return Message()
 
  @freeze_time("2020-08-15")
  def test_time_measurement(self):
    ping_msg = self.create_message()
    ping_msg.start()
    ping_msg.end()
    assert ping_msg.time() == 0

  def test_time_now_mock(self):
    freezer = freeze_time("2020-11-08 12:00:01")
    freezer.start()
    start = datetime.now()
    assert datetime.now() == datetime(2020, 11, 8, 12, 0, 1)
    freezer.stop()
    freezer = freeze_time("2020-11-08 12:00:02")
    freezer.start()
    stop = datetime.now()
    dt_ms = stop.timestamp()*1000 - start.timestamp()*1000
    assert dt_ms == 1000
    freezer.stop()

  def test_serialization(self):
    freezer = freeze_time("2020-11-08 12:00:00")
    freezer.start()

    ping_msg = self.create_message()
    ping_msg.start()
    msg = ping_msg.json()
    server_msg = Message(json.loads(msg))
    freezer = freeze_time("2020-11-08 12:00:01")
    freezer.start()
    server_msg.end()
    df = server_msg.time()
    assert server_msg.time() == 1.0



    