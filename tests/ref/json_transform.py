from mock import patch, Mock
import pytest
from pytest_mock import mocker
from transports.udp_transport import UDPTransport

class TestSimpleClientSeverPing:

  def create_client(self):
    self.udp_client = UDPTransport()

  def create_server(self, protocol, ip, port):
    self.udp_server = UDPTransport()
    self.udp_server.connect('127.0.0.1', 20001)
  
  def test_ping(self, mocker):
    pass
