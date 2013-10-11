from testify import *

DB_PATH = '/tmp/spider.db'
STORAGE_DIR = '/tmp/storage'

class DefaultTestCase(TestCase):

    @class_setup
    def init_the_variable(self):
        from frontend import CreateDBMode
        import os

        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)

        if not os.path.exists(STORAGE_DIR):
            os.mkdir(STORAGE_DIR)

        fend = CreateDBMode(dict(db_path=DB_PATH))
        fend.run()

    def test_single_mode(self):
        from frontend import SingleMode

        fend = SingleMode(dict(
            db_path=DB_PATH,
            storage_dir=STORAGE_DIR,
            url='http://github.com',
        ))
        fend.run()

if __name__ == '__main__':
    run()