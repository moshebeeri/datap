from mock import patch, Mock
import pytest
from pytest_mock import mocker
from commands.client import Client
from commands.server import Server

class Transport:
  pass

class TestSimpleClientSeverPing:
  def create_client(self, protocol, ip, port):
    return Client(Transport())

  def create_server(self, protocol, ip, port):
    return Server(Transport())
  
  @patch('commands.client.Client.ping', return_value=1)
  def test_ping(self, mocker):
    # mocker.patch('commands.client.Client.ping', return_value=1)

    client = self.create_client( "UDP", "127.0.0.1", 0)
    server = self.create_server( "UDP", "127.0.0.1", 0)
    time = client.ping()
    assert time <= 1

