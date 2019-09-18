from __future__ import print_function
import six.moves.SimpleHTTPServer
import six.moves.socketserver


PORT = 8000


class Handler(six.moves.SimpleHTTPServer.SimpleHTTPRequestHandler):
    pass


Handler.extensions_map['.json'] = 'application/json'

httpd = six.moves.socketserver.TCPServer(("", PORT), Handler)


print("serving at port {port}".format(port=PORT))
httpd.serve_forever()
