import sqlite3

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
        from spider import Document
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