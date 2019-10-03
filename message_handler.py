import struct

class MessageHandler():

	_structFormat = '>H'
	_maxHeaderLength = 2 #2 bytes

	def _createMessageWithHeader(self, encodedData, meta):
		#print('encodedData:', encodedData)
		encodedHeader = 'len:{}\n{}'.format(len(encodedData), meta).encode(encoding='utf-8')
		#print('header:', 'len:{}\n{}'.format(len(encodedData), meta))
		headerLength = struct.pack(self._structFormat, len(encodedHeader))
		#print('headerLength:', len(encodedHeader))
		return headerLength + encodedHeader + encodedData


	def createMessage(self, message, messageMeta, isByteData=False):
		if message == None:
			return None
		encodedData = message
		if not isByteData:
			encodedData = str(message).encode(encoding='utf-8')
		return self._createMessageWithHeader(encodedData, messageMeta if messageMeta else None)

	def _readAll(self, socket, expectedDataLength):
		buf = bytearray(expectedDataLength)
		view = memoryview(buf)
		dataSizeRead = 0
		while dataSizeRead < expectedDataLength:
			k = self._safeRead(socket, view, dataSizeRead, expectedDataLength)
			dataSizeRead += k
		return buf

	def _safeRead(self, socket, buffer, bufferOffset, totalDataLength):
		try:
			return socket.recv_into(buffer[bufferOffset:], totalDataLength - bufferOffset)
		except BlockingIOError:
			# Resource temporarily unavailable (errno EWOULDBLOCK)
			print('Resource temporarily unavailable: trying again...')
			return self._safeRead(socket, buffer, bufferOffset, totalDataLength)

	def readMessage(self, socket, bufferSize=4096):
		if socket == None:
			print('No socket provided')
			return None
		headerLength = struct.unpack(self._structFormat, socket.recv(self._maxHeaderLength))[0]
		#print('headerLength:', headerLength)
		headers = socket.recv(headerLength).decode("utf-8").splitlines()
		#print('headers:', headers, 'length:', len(headers))
		dataLength = int(headers[0].split(':')[1])
		#print('dataLength:', dataLength)
		#data = socket.recv(int(dataLength))
		data = self._readAll(socket, dataLength)
		#print('data:', data.decode("utf-8"))

		message = Message(headers[1], data)
		return message


class Message():

	header = data = None

	def __init__(self, header, data):
		self.header = header
		self.data = data

	def __str__(self):
		return self.header, self.data