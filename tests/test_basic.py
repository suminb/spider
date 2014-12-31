from pytest import *

DB_PATH = '/tmp/spider.db'
STORAGE_DIR = '/tmp/storage'

from spider.database import Database
import os

@fixture
def setup():
    from frontend import CreateDBMode

    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    if not os.path.exists(STORAGE_DIR):
        os.mkdir(STORAGE_DIR)

    fend = CreateDBMode(dict(db_path=DB_PATH))
    fend.run()


def test_db_operations(setup):
    with Database(DB_PATH) as db:
        assert db.url_count == 0
        assert not db.has_url('any url')

        db.insert_urls(['http://gmail.com', 'http://facebook.com'])
        assert db.url_count == 2
        assert db.has_url('http://gmail.com')


def test_single_mode(setup):
    from frontend import SingleMode

    url = 'http://github.com'

    fend = SingleMode(dict(
        db_path=DB_PATH,
        storage_dir=STORAGE_DIR,
        url=url,
    ))
    fend.run()

    with Database(DB_PATH) as db:
        assert db.has_url(url)
        assert db.fetched_url(url)

        db.delete_url(url)
        assert not db.has_url(url)

