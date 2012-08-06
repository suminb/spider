
import urllib2

class FetchTask:
    
    USER_AGENT = 'Spider v0.1'
    REQUEST_TIMEOUT = 10

    def __init__(self, url):
        self.url = url

    def run(self, proxy=None):
        print FetchTask.fetch_url(self.url, proxy)

    @staticmethod
    def open_url(url, proxy=None):
        if proxy != None:
            opener = urllib2.build_opener(proxy.proxy_handler)
        else:
            opener = urllib2.build_opener()
        opener.addheaders = [('User-agent', FetchTask.USER_AGENT)]

        return opener.open(url, timeout=FetchTask.REQUEST_TIMEOUT)

    @staticmethod
    def fetch_url(url, proxy=None):
        f = FetchTask.open_url(url, proxy)
        content = f.read().decode('utf-8')
        f.close()

        return content

class TaskDispatcher:
    def dispatch(self, task):
        """
        task: an instance of FetchTask
        """
        raise Exception('Not implemented')

class Proxy:

    USER_AGENT = 'Spider v0.1'
    REQUEST_TIMEOUT = 10

    def __init__(self, type_, host, port):
        """
        type: HTTP, HTTPS
        host: domain name or IP address of proxy server
        port: port number
        """
        self.type = type_
        self.host = host
        self.port = port

    @property
    def proxy_handler(self):
        proxy_url = '%s://%s:%d' % (self.type, self.host, self.port)
        return urllib2.ProxyHandler({'http': proxy_url})

    @staticmethod
    def open_url(self, url, fetch_task):
        proxy_handler = urllib2.ProxyHandler({'http': proxy})
        opener = urllib2.build_opener(proxy_handler)
        opener.addheaders = [('User-agent', fetch_task.USER_AGENT)]

        return opener.open(url, timeout=fetch_task.REQUEST_TIMEOUT)

    @staticmethod
    def fetch_url(self, url, fetch_task):
        f = Proxy.open_url(url)
        content = f.read().decode('utf-8')
        f.close()

        return content
        