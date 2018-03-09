import socket
import socketserver
import logging


class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass


def start_service(host, port, service, request_handler):
    """
    Start a service.
    ----------------
    host [str]: address of host for the TCP-Server
    port [int]: port for the TCP-Server
    service: service run by the TCP-Server, must subclass `BaseService`
    request_handler: request handler for the TCP-Server, must subclass `BaseRequestHandler`
    """
    assert isinstance(service, BaseService)
    server = ThreadingTCPServer((host, port), request_handler)
    # add the service to the server to access it for requests
    server.service = service
    # add the server to the service to call finish after all requests are processed
    service.server = server
    # wrap in a try except and serialize the current state on keyboard interrupt
    try:
        server.serve_forever()
    except KeyboardInterrupt as e:
        service.serialize_status(True)
        service.shutdown_server()
        raise e
    except Exception as e:
        raise e


class BaseRequestHandler(socketserver.StreamRequestHandler):
    """
    Base class for request handler.
    Must override `format_request` and `format_response`.
    """
    def format_request(self, request):
        """
        Format the client-side request.
        Must return string.
        """
        raise AttributeError("BaseRequestHandler does not implement format_request")

    def format_response(self, response):
        """
        Format the client-side response.
        Must return string.
        """
        raise AttributeError("BaseRequestHandler does not implement format_response")

    def handle(self):
        """
        Handle request.
        """
        request = self.format_request(self.rfile.readline().strip())
        response = self.format_response(self.server.service.process_request(request))
        self.wfile.write(bytes(response + "\n", "utf8"))


class BaseService(object):
    """
    Base service.
    Must override `process_request`
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.server = None  # must be monkey-patched
        self.server_is_running = True

    def process_request(self, request):
        """
        Process incoming request.
        """
        raise AttributeError("BaseService does not implement process request")

    def shutdown_server(self):
        """
        Shutdown the server after all requests are processed.
        """
        if self.server is None:
            raise RuntimeError("Cannot call shutdown; invalid server")
        self.logger.info(" Shutting down connection to server %s:%i" % self.server.server_address)
        self.server.shutdown()
        self.server.server_close()
        self.server_is_running = False

    def serialize_status(self, from_interrupt=False):
        """
        Serialize status of the service, default dummy implementation.
        """
        if from_interrupt:
            self.logger.info(" serialize_status called after interrupt")
        else:
            self.logger.info(" serialize_status called after regular shutdown")
        self.logger.info(" Base implementation - doing nothing")


class BaseClient(object):
    """
    Base client.
    Must override `format_request` and `format_response`.
    """
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def format_request(self, request):
        """
        Format incoming request.
        Must return string.
        """
        raise AttributeError("BaseClient does not implement format request")

    def format_response(self, response):
        """
        Format incoming response.
        """
        raise AttributeError("BaseClient does not implement format response")

    def request(self, request=None):
        with socket.create_connection((self.host, self.port)) as sock:
            sock.sendall(bytes(self.format_request(request) + '\n', 'utf-8'))
            response = self.format_response(str(sock.recv(1024), 'utf-8'))
        return response
