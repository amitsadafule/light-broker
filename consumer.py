import selectors
import socket
import types
import uuid
from message_handler import MessageHandler, Message

class Consumer():

	_BINDING_IP = '127.0.0.1'
	_BINDING_PORT = 65432
	_socketSelector = None
	_CONNECTION_ID = None
	_messageHandler = None
	_MAX_BUFFER_SIZE = 4096

	def __init__(self):
		self._CONNECTION_ID = uuid.uuid1()
		self._socketSelector = selectors.DefaultSelector()
		self._messageHandler = MessageHandler()

		broker_addr = (self._BINDING_IP, self._BINDING_PORT)
		print('starting connection', self._CONNECTION_ID, 'to', broker_addr)
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.setblocking(False)
		sock.connect_ex(broker_addr)
		events = selectors.EVENT_READ
		data = types.SimpleNamespace(connectionId = self._CONNECTION_ID, inb = b'')
		self._socketSelector.register(sock, events, data=data)
		self._checkForMessages();

	def _checkForMessages(self):
		while True:
			events = self._socketSelector.select(timeout=None)
			if events:
				for key, mask in events:
					self._serviceConnection(key, mask)

			'''if not self._socketSelector.get_map():
				break'''

	def _writeToFile(self, message):
		fileName = message.header.split('=')[1]
		with open('output/'+fileName,"wb") as f:
			f.write(message.data)

	def _serviceConnection(self, key, mask):
		sock = key.fileobj
		data = key.data
		if mask & selectors.EVENT_READ:
			message = self._messageHandler.readMessage(sock)
			#recv_data = sock.recv(1024)  # Should be ready to read
			if message:
				#print('received', repr(message), 'from connection', data.connectionId)
				self._writeToFile(message)
			if not message:
				print('closing connection', data.connectionId)
				self._socketSelector.unregister(sock)
				sock.close()

c = Consumer()

