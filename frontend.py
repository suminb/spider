from spider import Document, FetchTask, Storage
from spider.database import Database

import getopt
import time
import sys, os
import logging

logger = logging.getLogger('spider')
handler = logging.FileHandler('spider.log')
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
#logger.addHandler(logging.StreamHandler(sys.stdout))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

fend = None

def reduce_report(row1, row2):
    # Assuming row1 and row2 have share the same keys
    r = {}
    for key in row1:
        r[key] = row1[key] + row2[key]

    return r


# FIXME: Temporary
def fetch_unfetched_urls(limit, opts):
    with Database(opts["db_path"], logger=logger) as db:
        curs = db.cursor
        curs.execute("SELECT url FROM document WHERE timestamp IS NULL LIMIT ?", (int(limit),))
        
        return map(lambda u: u[0], curs.fetchall())


def fetch_url(args):
    url, opts = args

    import urllib2
    import random
    import thread
    import contextlib

    # thread ID
    tid = thread.get_ident()

    with contextlib.nested(
        Database(opts["db_path"], logger=logger),
        open(opts["log_path"], "a")) as (db, log):

        storage = Storage('file')

        url_entry = db.fetch_document(url)
        has_url = (url_entry != None)

        request_succeeded = 0
        new_urls_count = 0

        fetch_flag = True
        if has_url:
            # TODO: Check if timestamp was too long time ago
            pass

        if not fetch_flag:
            logger.info("URL entry (%s) already exists. Skipping..." % url)
        else:

            task = FetchTask(url, logger=logger)

            # FIXME: Revise semantics
            if fend != None:
                task.proxy_factory = fend.proxy_factory

            try:
                url_entry = task.run(db, opts)
                request_succeeded = 1

                storage.save(url, url_entry, opts)

                if has_url:
                    db.update_document(url_entry)
                else:
                    db.insert_document(url_entry)

                if 'url_patterns' in opts:
                    for url_pattern in opts["url_patterns"]:
                        urls = url_entry.extract_urls(url_pattern)
                        new_urls_count += len(urls)
                        db.insert_urls(urls)
                    logger.info("[%x] Found %d URLs in %s.\n" % (tid, new_urls_count, url))


                if "process_content" in opts:
                    opts["process_content"](url_entry)

            except Exception as e:
                logger.exception(e)
                db.delete_url(url)

            finally:
                sys.stdout.write('+' if request_succeeded != 0 else '-')
                sys.stdout.flush()


        # number of bytes of the fetched url_entry
        fetched_size = len(url_entry.content) if url_entry != None and url_entry.content != None else 0

        return {"count": 1,
                "succeeded": request_succeeded,
                'new_urls_count': new_urls_count,
                'fetched_size': fetched_size,
            }


class Frontend:
    def __init__(self, opts, logger=logging):
        self.opts = opts
        self.logger = logger
        self.proxy_factory = None

        # Default values
        if not 'log_path' in opts:
            self.opts['log_path'] = 'spider.log'

        # shared across multiple threads
        self.shared = {}

    def run(self):
        raise Exception("Not implemented")


class SingleMode(Frontend):
    def __int__(self, opts, logger):
        super(Frontend, self).__init__(opts, logger)

    def run(self):
        start_time = time.time()

        report = fetch_url((self.opts["url"], self.opts))

        # print an empty line after the execution
        print()

        end_time = time.time()
        report["time_elapsed"] = end_time - start_time # in seconds

        ReportMode.generate_report(self.opts["db_path"], report, self.opts)


class MultiThreadingMode(Frontend):
    def __int__(self, opts, logger):
        super(Frontend, self).__init__(opts, logger)

        from hallucination import ProxyFactory
        self.proxy_factory = ProxyFactory(
            config=dict(db_uri=opts['hallucination_db_uri']),
            logger=logger,
        )

    def __del__(self):
        pass

    def run(self):
        from multiprocessing.pool import ThreadPool

        start_time = time.time()

        unfetched_urls = fetch_unfetched_urls(self.opts["n_urls"], self.opts)
        pool = ThreadPool(self.opts["n_proc"])
        result = pool.map(fetch_url, map(lambda u: (u, self.opts), unfetched_urls))

        report = {}
        if result != []:
            report = reduce(reduce_report, result)

        end_time = time.time()
        report["time_elapsed"] = end_time - start_time # in seconds

        ReportMode.generate_report(self.opts["db_path"], report, self.opts)


class CreateDBMode(Frontend):
    def __int__(self, opts, logger):
        super(Frontend, self).__init__(opts, logger)

    def run(self):
        with Database(self.opts["db_path"], logger=logger) as db:
            with open("scheme.txt") as f:
                # FIXME: This may break in some cases
                for sql in f.read().split(";"):
                    db.execute(sql)


class ReportMode(Frontend):
    def __int__(self, opts, logger):
        super(Frontend, self).__init__(opts, logger)

    def run(self):
        if "db_path" not in self.opts:
            raise Exception("Database path is not specified.")
        else:
            ReportMode.generate_report(self.opts["db_path"])

    @staticmethod
    def human_readable_size(size):
        if size < 1024:
            return "%d bytes" % size
        
        elif size < 1024**2:
            return "%.02f KB" % (float(size) / 1024)

        elif size < 1024**3:
            return "%.02f MB" % (float(size) / 1024**2)

        else:
            return "%.02f GB" % (float(size) / 1024**3)

    @staticmethod
    def generate_report(db_path, session_report=None, opts=None):
        """Prints out a status report to standard output. This function may be called from outside this class."""
        
        from spider.database import Database

        with Database(db_path, logger=logger) as db:
            url_count = db.url_count
            fetched_url_count = db.fetched_url_count

            if session_report != None and ("count" in session_report):
                print()
                print("-[ Spider Report: This session ]------------------------------------")
                print("  Number of fetch requests sent out: %d" % session_report["count"])
                print("  Number of successful fetches: %s" % session_report["succeeded"])
                print("  Time elapsed: %.03f sec" % session_report["time_elapsed"])
                print("  Fetching speed: %.03f pages/sec" % (session_report["succeeded"] / session_report["time_elapsed"]))
                print("  Total size of fetched documents: %s" % ReportMode.human_readable_size(session_report['fetched_size']))
                print("  Number of newly found URLs: %d" % session_report['new_urls_count'])

            print()
            print("-[ Spider Report: Overall summary ]------------------------------------")
            print("  Total number of URLs: %d" % url_count)
            print("  Number of fetched URLs: %d" % fetched_url_count)
            if url_count > 0:
                print("  Progress: %.02f%%" % (100.0 * fetched_url_count / url_count))
                print("  Database file size: %s" % ReportMode.human_readable_size(os.path.getsize(db_path)))


class ProfileMode(Frontend):
    def __init__(self, opts, logger):
        #super(ProfileMode, self).__init__(opts, logger)
        Frontend.__init__(self, opts, logger)

        # If there is nothing to fetch, exit
        # Figure out # of URLs to fetch
        # Figure out optimal # of threads
        # Continuously run multithreading mode
        
        profile = __import__(self.opts['profile'])

        # TODO: Any better way to handle this?
        self.opts['n_urls'] = profile.URLS
        self.opts['n_proc'] = profile.THREADS
        self.opts['db_path'] = profile.DB_URI
        self.opts['url_patterns'] = profile.URL_PATTERNS
        self.opts['storage_dir'] = profile.STORAGE_DIR
        self.opts['process_content'] = profile.process_content
        self.opts['hallucination_db_uri'] = profile.HALLUCINATION_DB_URI
        self.opts['user_agent'] = profile.USER_AGENT

        with Database(self.opts['db_path'], logger=logger) as db:
            db.insert_urls(profile.ENTRY_POINTS)
        

    def run(self):
        multimode = MultiThreadingMode(self.opts, logger)
        multimode.run()


def parse_args(args):
    optlist, args = getopt.getopt(args, "u:n:t:d:p:smag", ("create-db", "single", "generate-report", "auto"))

    opts = {}

    for o, a in optlist:
        if o == '-n':
            opts['n_urls'] = int(a)

        elif o == '-t':
            opts['n_proc'] = int(a)

        elif o == '-d':
            opts['db_path'] = a

        elif o in ('-u', '--url'):
            opts['url'] = a

        elif o == '-p':
            opts['run_mode'] = 'profile'
            opts['profile'] = a

        elif o == '--create-db':
            opts['run_mode'] = 'create_db'

        elif o in ('-s', '--single'):
            opts['run_mode'] = 'single'
            opts['n_urls'] = 1

        elif o in ('-m', '--multithreading'):
            opts['run_mode'] = 'multithreading'

        elif o in ('-a', '--auto'):
            opts['run_mode'] = 'auto'

        elif o in ('-g', '--generate-report'):
            opts['run_mode'] = 'generate_report'

    return opts


def validate_runtime_options(opts):
    if 'run_mode' not in opts:
        return (False, 'Run mode is not specified')

    elif (opts['run_mode'] == 'create_db'):
        if ('db_path' not in opts):
            return (False, 'SQLite3 database path must be supplied (-d)')
        else:
            return (True, '')

    elif (opts['run_mode'] == 'single') and ('db_path' in opts) and ('url' in opts):
        return (True, '')

    elif (opts['run_mode'] == 'multithreading'):
        if ('db_path' not in opts):
            return (False, 'SQLite3 database path must be supplied (-d)')

        elif ('n_urls' not in opts):
            return (False, 'Specify the number of URLs to fetch (-n)')

        elif ('n_proc' not in opts):
            return (False, 'Specify the number of threads (-t)')

        else:
            return (True, '')

    elif (opts['run_mode'] == 'profile'):
        if ('profile' not in opts):
            return (False, 'Specify a profile to run (-p)')
        else:
            return (True, '')

    return (False, 'Unclassified error')


def main():
    opts = parse_args(sys.argv[1:])

    valid, message = validate_runtime_options(opts)

    if valid:
        run_mode = opts['run_mode']

        fc = {
            'create_db': CreateDBMode,
            'single': SingleMode,
            'multithreading': MultiThreadingMode,
            'generate_report': ReportMode,
            'profile': ProfileMode,
        }

        global fend
        fend = fc[run_mode](opts, logger=logger)
        fend.run()

    else:
        sys.stderr.write(message + '\n')

if __name__ == '__main__':
    main()
