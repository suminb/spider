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

    def __exit__(self):
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
        curs = self.cursor
        curs.execute("SELECT * FROM document WHERE url=?", (url,))
        row = curs.fetchone()

        return row != None

    def insert_urls(self, urls):
        for url in urls:
            try:
                self.execute("INSERT INTO document (url) VALUES (?)", (url,), False)
            except sqlite3.IntegrityError as e:
                # Simply ignore it if url already exists
                pass
        self.commit()

    def insert_document(self, url, mimetype, timestamp, content, commit=True):
        self.execute("INSERT INTO document VALUES (?, ?, ?, ?)", (url, mimetype, timestamp, content), commit)

    def update_document(self, url, mimetype, timestamp, content, commit=True):
        self.execute("UPDATE document SET mime_type=?, last_fetched=?, content=? WHERE url=?", (mimetype, timestamp, content, url), commit)

    def fetch_document(self, url):
        curs = self.cursor
        curs.execute("SELECT * FROM document WHERE url=?", (url,))

        return curs.fetchone()

    def export(self):
        """Export documents to files."""
        pass


class FetchTask:
    
    USER_AGENT = 'Spider v0.1'
    REQUEST_TIMEOUT = 10

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
            if isinstance(db, Database):
                if db.has_url(url):
                    doc = db.fetch_document(url)
                    content = doc[3]
                    has_url = True

            if content == None:
                f = FetchTask.open_url(url, proxy)
                content = f.read().decode('utf-8')
                f.close()
                succeeded = True
                used_proxy = True

        except Exception, e:
            raise e
        finally:
            end_time = time.time()
            time_elapsed = long((end_time - start_time) * 1000)

            if isinstance(proxy, Proxy) and used_proxy:
                proxy.report_status(succeeded, time_elapsed)

            if isinstance(db, Database):
                if has_url:
                    db.update_document(url, '', datetime.datetime.now(), content)
                else:
                    db.insert_document(url, '', datetime.datetime.now(), content)

        return content


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

    def __init__(self, raw_content):
        self.raw_content = raw_content

    def extract_urls(self, url_pattern=None):
        """Returns a list of HTTP/S URLs in string format."""

        if url_pattern == None:
            url_pattern = self.url_pattern

        return re.findall(url_pattern, self.raw_content)



# TODO: Implement URL extractor
# TODO: Implement URL filter
# TODO: Implement Write class that abstracts the process of storing fetched pages
