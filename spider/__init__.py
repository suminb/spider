
__author__ = 'Sumin Byeon'
__version__ = '0.9.1'

from .database import Database
from threading import Thread
from bs4 import BeautifulSoup, SoupStrainer
from .utils import make_absolute_url

import re
import datetime
import time
import requests
import logging


class FetchTask:
    USER_AGENT = 'Spider v%s' % __version__
    REQUEST_TIMEOUT = 10

    def __init__(self, url, logger=logging):
        self.url = url
        self.proxy_factory = None
        self.logger = logger

    def run(self, db=None, opts=None):
        if 'user_agent' in opts:
            self.USER_AGENT = opts['user_agent']

        return FetchTask.fetch_url(self.url, self.proxy_factory, db, opts)

    @staticmethod
    def fetch_url(url, proxy_factory=None, db=None, opts=None):

        content = None
        has_url = False
        succeeded = False
        document = None
        raw_content = None

        try:
            if proxy_factory != None:
                req = proxy_factory.make_request(url)
            else:
                req = requests.get(url)

            raw_content = req.text

        except Exception as e:
            self.logger.exception(e)

        succeeded = True

        document = Document(url, req.headers['content-type'],
            datetime.datetime.now(), raw_content)

        if db != None:
            db.mark_as_fetched(document)

        return document


class URL:
    def __init__(self, key, url, timestamp, fetched_size):
        self.url = url



class Document:

    # FIXME: This does not cover all possible forms of URLs, but we'll
    # stick with this for now.
    url_pattern = r"https?:\/\/[\da-z\.-]+\.[a-z\.]{2,6}[\/\w \.\-\+]*\/?"

    def __init__(self, url, mime_type, timestamp, content):
        self.url = url
        self.mime_type = mime_type
        self.timestamp = timestamp
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
            import hashlib

            storage_dir = os.path.abspath(opts['storage_dir'])

            if not os.path.exists(storage_dir):
                os.mkdir(storage_dir)

            file_name = hashlib.sha1(url).hexdigest()
            file_path = os.path.join(storage_dir, file_name)

            with open(file_path, 'w') as f:
                f.write('<!-- %s -->\n' % url)
                f.write(document.content.encode('utf-8'))

            return file_name

        elif self.engine_type == 'sqlite3':
            pass
        else:
            pass

