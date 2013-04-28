from proxy import Proxy
from database import Database
from Queue import Queue
from threading import Thread
from bs4 import BeautifulSoup, SoupStrainer
from utils import make_absolute_url
from __init__ import __version__

import urllib2
import re
import datetime
import time


class FetchTask:
    
    USER_AGENT = 'Spider v%s' % __version__
    REQUEST_TIMEOUT = 10

    def __init__(self, url):
        self.url = url

    def run(self, proxy=None, db=None, opts=None):
        return FetchTask.fetch_url(self.url, proxy, db, opts)

    @staticmethod
    def open_url(url, proxy=None):
        if isinstance(proxy, Proxy):
            opener = urllib2.build_opener(proxy.proxy_handler)
        else:
            opener = urllib2.build_opener()
        opener.addheaders = [("User-agent", FetchTask.USER_AGENT)]

        return opener.open(url, timeout=FetchTask.REQUEST_TIMEOUT)

    @staticmethod
    def fetch_url(url, proxy=None, db=None, opts=None):

        start_time = time.time()
        content = None
        has_url = False
        succeeded = False
        used_proxy = False

        try:
            f = FetchTask.open_url(url, proxy)
            raw_content = f.read().decode("utf-8") # content is unicode type at this point

            f.close()
            succeeded = True
            used_proxy = proxy != None

        except Exception as e:
            raise e

        finally:
            end_time = time.time()
            time_elapsed = long((end_time - start_time) * 1000)

            if isinstance(proxy, Proxy) and used_proxy:
                proxy.report_status(succeeded, time_elapsed)

        return Document(url, None, datetime.datetime.now(), raw_content)


class URL:
    def __init__(self, key, url, last_fetched, fetched_size):
        self.url = url



class Document:

    # FIXME: This does not cover all possible forms of URLs, but we'll
    # stick with this for now.
    url_pattern = r"https?:\/\/[\da-z\.-]+\.[a-z\.]{2,6}[\/\w \.\-\+]*\/?"

    def __init__(self, url, mime_type, last_fetched, content):
        self.url = url
        self.mime_type = mime_type
        self.last_fetched = last_fetched
        self.content = content

        #if content != None:
        #    open('debug.txt', 'w').write(content)

    def __getstate__(self):
        return self.__dict__.copy()

    def __setstate__(self, dict):
        self.__dict__.update(dict)

    def extract_urls(self, url_pattern=None):
        """Returns a list of HTTP/S URLs in string format."""

        if url_pattern == None:
            url_pattern = self.url_pattern

        soup = BeautifulSoup(self.content, parse_only=SoupStrainer("a"))

        # Find all anchor tags
        urls = soup.find_all("a")

        # Filter out anchor tags without "href" attribute
        urls = filter(lambda a: a.get("href") != None, urls)

        # Make the all absolute URLs
        urls = map(lambda a: make_absolute_url(self.url, a.get("href")), urls)

        # Filter URLs that match url_pattern
        urls = filter(lambda u: re.match(url_pattern, u) != None, urls)

        return urls


class Storage:
    """Represents a storage engine."""

    engine_types = ('file', 'sqlite3')

    def __init__(self, engine_type):
        if engine_type in self.engine_types:
            self.engine_type = engine_type
        else:
            raise Exception("Storage engine type '%s' is not supported." % engine_type)

    def save(self, url, document, opts):
        if self.engine_type == 'file':
            import sys, os
            import base64

            storage_dir = os.path.abspath(opts['storage_dir'])

            if not os.path.exists(storage_dir):
                os.mkdir(storage_dir)

            # Decided to use a base32 encoding because some file systems are case-insensitive.
            file_name = base64.b32encode(url)
            file_path = os.path.join(storage_dir, file_name)

            with open(file_path, 'w') as f:
                f.write(document.content)

            return file_name

        elif self.engine_type == 'sqlite3':
            pass
        else:
            pass

