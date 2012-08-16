from proxy import *

import urllib2
import re
import sqlite3
import datetime

class Database:

    def __init__(self, file_name):
        """
        file_name -- Name of sqlite3 database file.
        """
        self.file_name = file_name

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    @property
    def cursor(self):
        return self.conn.cursor()

    @property
    def connection(self):
        return self.conn

    def connect(self):
        self.conn = sqlite3.connect(self.file_name)
        return self.conn

    def close(self):
        self.conn.close()

    def execute(self, query, params, commit=True):
        """
        query -- An SQL statement
        params -- List type instance
        """
        curs = self.conn.cursor()
        curs.execute(query, params)

        if commit:
            self.conn.commit()

    def commit(self):
        self.conn.commit()

    #
    # Spider specific functions
    #

    def has_url(self, url):
        """Indicates if the url exists in document table."""
        curs = self.cursor
        curs.execute("SELECT * FROM document WHERE url=?", (url,))
        row = curs.fetchone()

        return row != None

    def fetched_url(self, url):
        """Indicates if the url is already fetched."""
        self.execute("SELECT * FROM document WHERE url=? AND last_fetched IS NOT NULL", (url,))
        row = self.cursor.fetchone()

        return row != None

    def insert_urls(self, urls):
        for url in urls:
            try:
                self.execute("INSERT INTO document (url) VALUES (?)", (url,), False)
            except sqlite3.IntegrityError as e:
                # Simply ignore it if url already exists
                pass
        self.commit()

    def delete_url(self, url, commit=True):
        self.execute("DELETE FROM document WHERE url=?", (url,), commit)

    def insert_document(self, document, commit=True):
        self.execute("INSERT INTO document VALUES (?, ?, ?, ?)",
            (document.url, document.mime_type, document.last_fetched, document.content), commit)

    def update_document(self, document, commit=True):
        self.execute("UPDATE document SET mime_type=?, last_fetched=?, content=? WHERE url=?",
            (document.mime_type, document.last_fetched, document.content, document.url), commit)

    def fetch_document(self, url):
        curs = self.cursor
        curs.execute("SELECT * FROM document WHERE url=?", (url,))

        row = curs.fetchone()
        if row == None:
            return None
        else:
            return Document(row[0], row[1], row[2], row[3])

    def export(self):
        """Export documents to files."""
        pass


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
