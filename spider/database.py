import sqlite3
import logging

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

    def execute(self, query, params=(), commit=True):
        """
        query -- An SQL statement
        params -- List type instance
        """
        curs = self.conn.cursor()
        curs.execute(query, params)

        if commit:
            self.conn.commit()

    def fetch_one(self, query, params=()):
        curs = self.conn.cursor()
        curs.execute(query, params)

        return curs.fetchone()

    def fetch_all(self, query, params=()):
        curs = self.conn.cursor()
        curs.execute(query, params)

        return curs.fetchall()

    def commit(self):
        self.conn.commit()

    #
    # Spider specific functions
    #

    @property
    def url_count(self):
        row = self.fetch_one("SELECT COUNT(url) FROM document")

        return row[0] if row != None else 0

    @property
    def fetched_url_count(self):
        row = self.fetch_one("SELECT COUNT(url) FROM document WHERE timestamp IS NOT NULL")

        return row[0] if row != None else 0

    def has_url(self, url):
        """Indicates if the url exists in document table."""
        row = self.fetch_one("SELECT * FROM document WHERE url=?", (url,))

        return row != None

    def fetched_url(self, url):
        """Indicates if the url is already fetched."""
        row = self.fetch_one("SELECT * FROM document WHERE url=? AND timestamp IS NOT NULL", (url,))

        return row != None

    def insert_urls(self, urls):
        for url in urls:
            try:
                self.execute("INSERT INTO document (url) VALUES (?)", (url,), False)
            except sqlite3.IntegrityError as e:
                logging.warning("URL '%s' already exists." % url)
        self.commit()

    def delete_url(self, url, commit=True):
        self.execute("DELETE FROM document WHERE url=?", (url,), commit)

    def insert_document(self, document, commit=True):
        self.execute("INSERT INTO document (url, mime_type, timestamp) VALUES (?, ?, ?)",
            (document.url, document.mime_type, document.timestamp), commit)

    def update_document(self, document, commit=True):
        self.execute("UPDATE document SET mime_type=?, timestamp=? WHERE url=?",
            (document.mime_type, document.timestamp, document.url), commit)

    def fetch_document(self, url):
        from spider import Document
        
        row = self.fetch_one("SELECT * FROM document WHERE url=?", (url,))

        if row == None:
            return None
        else:
            return Document(row[0], row[1], row[2], row[3])

    def mark_as_fetched(self, document, commit=True):
        from datetime import datetime
        self.execute("UPDATE document SET timestamp=? WHERE url=?",
            (datetime.now(), document.url), commit)

    def export(self):
        """Export documents to files."""
        pass