"""Notification handling for Google Cloud Storage."""

import json
import logging

import webapp2


class objectChangePage(webapp2.RequestHandler):
    """Process notification events."""
    def get(self):
        logging.info("Get request to notification page.")
        self.response.write("Welcome to the notification app.")


    def post(self): 
        logging.debug( '%s\n\n%s', '\n'.join(['%s: %s' % x for x in self.request.headers.iteritems()]), self.request.body)
        
        if 'X-Goog-Resource-State' in self.request.headers:
            resource_state = self.request.headers['X-Goog-Resource-State']
            if resource_state == 'sync':
                logging.info('Sync message received.')
            else:
                an_object = json.loads(self.request.body)
                bucket = an_object['bucket']
                object_name = an_object['name']
                logging.critical('%s/%s %s', bucket, object_name, resource_state)
        else:
            logging.info("Other post.")

logging.getLogger().setLevel(logging.DEBUG)
app = webapp2.WSGIApplication([('/h/', objectChangePage)], debug=True)
