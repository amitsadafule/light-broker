class SimpleServer(gevent.server.StreamServer):

    def handle(self, socket, address):
        socket.sendall('hello and goodbye!')