import time
import urllib2

class Proxy:

    def __init__(self, type_, host, port):
        """
        type_ -- 'http' or 'https'
        host -- domain name or IP address of proxy server
        port -- port number
        """
        self.type = type_
        self.host = host
        self.port = int(port)

    def __str__(self):
        return "%s://%s:%d" % (self.type, self.host, self.port)

    @property
    def proxy_handler(self):
        """Simply returns a ProxyHandler instance."""
        proxy_url = '%s://%s:%d' % (self.type, self.host, self.port)
        return urllib2.ProxyHandler({'http': proxy_url})

    def report_status(self, succeeded, time_elapsed):
        """
        succeeded -- True or False
        time_elapsed -- Time took to complete the HTTP request via proxy in milliseconds
        """
        #print succeeded
        #print time_elapsed

