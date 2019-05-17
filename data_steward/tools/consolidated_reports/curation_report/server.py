import SimpleHTTPServer
import SocketServer


PORT = 8000


class Handler(SimpleHTTPServer.SimpleHTTPRequestHandler):
    pass


Handler.extensions_map['.json'] = 'application/json'

httpd = SocketServer.TCPServer(("", PORT), Handler)


print("serving at port {port}".format(port=PORT))
httpd.serve_forever()
