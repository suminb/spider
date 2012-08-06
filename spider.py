from proxy import *
import urllib2

class FetchTask:
    
    USER_AGENT = 'Spider v0.1'
    REQUEST_TIMEOUT = 10

    def __init__(self, url):
        self.url = url

    def run(self, proxy=None):
        return FetchTask.fetch_url(self.url, proxy)

    @staticmethod
    def open_url(url, proxy=None):
        if isinstance(proxy, Proxy):
            opener = urllib2.build_opener(proxy.proxy_handler)
        else:
            opener = urllib2.build_opener()
        opener.addheaders = [('User-agent', FetchTask.USER_AGENT)]

        return opener.open(url, timeout=FetchTask.REQUEST_TIMEOUT)

    @staticmethod
    def fetch_url(url, proxy=None):

        start_time = time.time()
        content = None
        succeeded = False

        try:
            f = FetchTask.open_url(url, proxy)
            content = f.read().decode('utf-8')
            f.close()
            succeeded = True
        except Exception, e:
            raise e
        finally:
            end_time = time.time()
            time_elapsed = long((end_time - start_time) * 1000)

            if isinstance(proxy, Proxy):
                proxy.report_status(succeeded, time_elapsed)

        return content

class TaskDispatcher:
    def dispatch(self, task):
        """
        task -- an instance of FetchTask
        """
        raise Exception('Not implemented')
