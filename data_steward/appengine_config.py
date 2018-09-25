from google.appengine.ext import vendor

import socket
DEFAULT_FETCH_DEADLINE = 300
# Add any libraries installed in the "lib" folder.
# vendor.add('lib2')
vendor.add('lib')

# makes the http request deadline to 60 sec
socket.setdefaulttimeout(DEFAULT_FETCH_DEADLINE)
