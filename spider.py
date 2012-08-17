from proxy import Proxy
from database import Database

import urllib2
import re
import datetime
import time


class FetchTask:
    
    USER_AGENT = 'Spider v0.1'
    REQUEST_TIMEOUT = 15

    def __init__(self, url):
        self.url = url

    def run(self, proxy=None, db=None):
        return FetchTask.fetch_url(self.url, proxy, db)

    @staticmethod
    def open_url(url, proxy=None):
        if isinstance(proxy, Proxy):
            opener = urllib2.build_opener(proxy.proxy_handler)
        else:
            opener = urllib2.build_opener()
        opener.addheaders = [('User-agent', FetchTask.USER_AGENT)]

        return opener.open(url, timeout=FetchTask.REQUEST_TIMEOUT)

    @staticmethod
    def fetch_url(url, proxy=None, db=None):

        start_time = time.time()
        content = None
        has_url = False
        succeeded = False
        used_proxy = False

        try:
            f = FetchTask.open_url(url, proxy)
            content = f.read().decode('utf-8')
            f.close()
            succeeded = True
            used_proxy = True

        except Exception as e:
            raise e

        finally:
            end_time = time.time()
            time_elapsed = long((end_time - start_time) * 1000)

            if isinstance(proxy, Proxy) and used_proxy:
                proxy.report_status(succeeded, time_elapsed)

        return Document(url, None, datetime.datetime.now(), content)


class TaskDispatcher:
    def dispatch(self, task):
        """
        task -- an instance of FetchTask
        """
        raise Exception('Not implemented')


class Document:

    # FIXME: This does not cover all possible forms of URLs, but we'll
    # stick with this for now.
    url_pattern = r"https?:\/\/[\da-z\.-]+\.[a-z\.]{2,6}[\/\w \.\-\+]*\/?"

    def __init__(self, url, mime_type, last_fetched, content):
        self.url = url
        self.mime_type = mime_type
        self.last_fetched = last_fetched
        self.content = content

    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, dict):
        self.__dict__.update(dict)

    def url():
        doc = "The url property."
        def fget(self):
            return self._url
        def fset(self, value):
            self._url = value
        def fdel(self):
            del self._url
        return locals()
    url = property(**url())

    def mime_type():
        doc = "The mime_type property."
        def fget(self):
            return self._mime_type
        def fset(self, value):
            self._mime_type = value
        def fdel(self):
            del self._mime_type
        return locals()
    mime_type = property(**mime_type())

    def last_fetched():
        doc = "The last_fetched property."
        def fget(self):
            return self._last_fetched
        def fset(self, value):
            self._last_fetched = value
        def fdel(self):
            del self._last_fetched
        return locals()
    last_fetched = property(**last_fetched())

    def content():
        doc = "The content property."
        def fget(self):
            return self._content
        def fset(self, value):
            self._content = value
        def fdel(self):
            del self._content
        return locals()
    content = property(**content())

    def extract_urls(self, url_pattern=None):
        """Returns a list of HTTP/S URLs in string format."""

        if url_pattern == None:
            url_pattern = self.url_pattern

        return re.findall(url_pattern, self.content)



# TODO: Implement URL extractor
# TODO: Implement URL filter
# TODO: Implement Write class that abstracts the process of storing fetched pages
