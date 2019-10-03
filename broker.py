import selectors
import socket
import types
import queue
import threading
from message_handler import MessageHandler
from datetime import datetime

class Broker():

	_BINDING_IP = '127.0.0.1'
	_BINDING_PORT = 65432
	_consumerSelector = None
	_QUEUE_SIZE = 2
	_messageQueue = None
	_messageHandler = None
	_MAX_BUFFER_SIZE = 4096

	def __init__(self):
		self._messageQueue = queue.Queue(self._QUEUE_SIZE)
		self._messageHandler = MessageHandler()

	def _initiateConsumerSelector(self): 
		self._consumerSelector = selectors.DefaultSelector()
		lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		lsock.bind((self._BINDING_IP, self._BINDING_PORT))
		lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		lsock.listen()
		print('listening on', (self._BINDING_IP, self._BINDING_PORT))
		lsock.setblocking(False)

		self._consumerSelector.register(lsock, selectors.EVENT_READ, data=None)
		self._acceptConnections()

	def _register(self, sock):
		conn, addr = sock.accept()  # Should be ready to read
		print('accepted connection from', addr)
		conn.setblocking(False)
		data = types.SimpleNamespace(addr=addr, outb=b'')
		events = selectors.EVENT_WRITE
		self._consumerSelector.register(conn, events, data=data)

	def _acceptConnections(self):
		while True:
			events = self._consumerSelector.select(timeout=None)
			for key, mask in events:
				if key.data is None:
					self._register(key.fileobj)
				else:
					self._service_connection(key, mask)

	def _sendAll(self, socket, data):
		dataLength = len(data)
		while dataLength > self._MAX_BUFFER_SIZE:
			self._safeSend(socket, data[:self._MAX_BUFFER_SIZE])
			dataLength -= self._MAX_BUFFER_SIZE
			data = data[self._MAX_BUFFER_SIZE:]

		#send remaining bytes
		self._safeSend(socket, data)


	def _safeSend(self, socket, data):
		try:
			sent = socket.send(data)  # Should be ready to write
		except BlockingIOError:
			# Resource temporarily unavailable (errno EWOULDBLOCK)
			print('Resource temporarily unavailable: trying again...')
			self._safeSend(socket, data)
		except BrokenPipeError:
			#ToDo try to send the entire data again
			print('connection lost')
			self._consumerSelector.unregister(socket)
			socket.close()

	def _service_connection(self, key, mask):
		sock = key.fileobj
		dataOverSocket = key.data
		if mask & selectors.EVENT_WRITE:
			'''if not dataOverSocket.outb:
				data = self._pop()'''
			data = self._pop()
			'''if self._messageQueue.qsize() == 0:
				print('ended at:', now = datetime.now().time())'''
			if data:
				#dataOverSocket.outb = data
				self._sendAll(sock, data)
				#print('sending', repr(data), 'to', dataOverSocket.addr)
				#dataOverSocket.outb = dataOverSocket.outb[sent:]

	def start(self):
		initThread = threading.Thread(target=self._initiateConsumerSelector)
		initThread.start()
		#self._initiateConsumerSelector()

	def push(self, data=None, meta=None, isByteData=False):
		if data == None:
			print('No data to push')
			pass
		packet = self._messageHandler.createMessage(data, meta, isByteData)
		#print('pushing', packet, ': current queue size', self._messageQueue.qsize())
		self._messageQueue.put(packet, block=True)

	def _pop(self):
		return self._messageQueue.get(block=True)