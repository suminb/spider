from database import Database
from proxy import Proxy
from spider import Document, FetchTask

#import curses
import threading
import getopt
import sys

# FIXME: Temporary
def load_proxy_list(file_name):
    import re
    with open(file_name) as f:
        return re.findall(r"(https?):\/\/([0-9a-z\.]+):(\d+)", f.read())

URL_PATTERNS = (
    r"http://messages.finance.yahoo.com/[A-Z][\/\w %=;&\.\-\+\?]*\/?",
    r"http://messages.finance.yahoo.com/search[\/\w %=;&\.\-\+\?]*\/?",
)

proxy_list = load_proxy_list("proxy_list.txt")

# FIXME: Temporary
lock = threading.Lock()

# FIXME: Temporary
status = {'processed_urls_count':0}
thread_status = {}

def fetch_unfetched_urls(limit, opts):
    with Database(opts["db_path"]) as db:
        curs = db.cursor
        curs.execute("SELECT url FROM document WHERE last_fetched IS NULL LIMIT ?", (limit,))
        
        return map(lambda u: u[0], curs.fetchall())

def reduce_report(row1, row2):
    return {'succeeded':row1['succeeded']+row2['succeeded'],
            'new_urls_count':row1['new_urls_count']+row2['new_urls_count'],
            'fetched_size':row1['fetched_size']+row2['fetched_size']}

def fetch_url(args):
    url, opts = args

    import urllib2
    import random
    import thread

    # thread ID
    tid = thread.get_ident()

    # randomly choose a proxy server
    pxy = random.choice(proxy_list)
    proxy = Proxy(pxy[0], pxy[1], pxy[2])

    lock.acquire()
    if not tid in thread_status:
        thread_status[tid] = {
                'url':None,
                'proxy':None,
                'message':None,
                'succeeded':0,
                'new_urls_count':0,
                'fetched_size':0
        }

    # URL currently being fetched
    thread_status[tid]['url'] = url
    thread_status[tid]['proxy'] = proxy
    thread_status[tid]['message'] = None
    lock.release()

    with Database(opts["db_path"]) as db:
        document = db.fetch_document(url)
        has_url = (document != None)

        request_succeeded = 0
        new_urls_count = 0        

        if document == None or document.content == None:
            print "[%x] Fetching %s via %s" % (tid, url, proxy)
            task = FetchTask(url)
            try:
                document = task.run(proxy, db)
                request_succeeded = 1

                if has_url:
                    db.update_document(document)
                else:
                    db.insert_document(document)

                for url_pattern in URL_PATTERNS:
                    urls = document.extract_urls(url_pattern)
                    new_urls_count += len(urls)
                    db.insert_urls(urls)
                print "[%x] Found %d URLs in %s." % (tid, new_urls_count, url)

            except urllib2.URLError as e:
                print 'URLError has been raised. Probably a proxy problem (%s).' % proxy
                print e
                #thread_status[tid]['message'] = "URLError has been raised. Probably a proxy problem (%s)" % proxy

            except urllib2.HTTPError as e:
                print 'HTTP error has occoured. Deleting url %s' % url
                #thread_status[tid]['message'] = "HTTP error has occoured. Deleting url %s" % url
                db.delete_url(url)

            except Exception as e:
                print 'Unclassified exception has occured: %s' % e
                #thread_status[tid]['message'] = "Unclassified exception has occured: %s" % e

        # number of bytes of the fetched document
        fetched_size = len(document.content) if document != None and document.content != None else 0

        lock.acquire()
        status['processed_urls_count'] += 1
        thread_status[tid]['new_urls_count'] = new_urls_count

        #if opts["run_mode"] == "multithreading":
        #    refersh_screen()
    
        lock.release()

        return {'succeeded':request_succeeded, 'new_urls_count':new_urls_count, 'fetched_size':fetched_size}

class Frontend:
    def __init__(self, opts):
        self.opts = opts

        # shared across multiple threads
        self.shared = {}

    def run(self):
        raise Exception("Not implemented")

    def prepare_curses(self):
        self.screen = curses.initscr()

    def cleanup_curses(self):
        curses.endwin()


class SingleMode(Frontend):
    def __int__(self, opts):
        super(Frontend, self).__init__(opts)

    def run(self):
        report = fetch_url((self.opts["url"], self.opts))
        ReportMode.generate_report(self.opts["db_path"], report, self.opts)


class MultiThreadingMode(Frontend):
    def __int__(self, opts):
        super(Frontend, self).__init__(opts)
        #super(Frontend, self).prepare_curses()

    def __del__(self):
        #super(Frontend, self).cleanup_curses()
        pass

    def run(self):
        from multiprocessing.pool import ThreadPool

        unfetched_urls = fetch_unfetched_urls(self.opts["n_urls"], self.opts)
        pool = ThreadPool(self.opts["n_proc"])
        result = pool.map(fetch_url, map(lambda u: (u, self.opts), unfetched_urls))
        report = reduce(reduce_report, result)

        ReportMode.generate_report(self.opts["db_path"], report, self.opts)


class CreateDBMode(Frontend):
    def __int__(self, opts):
        super(Frontend, self).__init__(opts)

    def run(self):
        with Database(self.opts["db_path"]) as db:
            with open("scheme.txt") as f:
                # FIXME: This may break in some cases
                for sql in f.read().split(";"):
                    db.execute(sql)


class ReportMode(Frontend):
    def __int__(self, opts):
        super(Frontend, self).__init__(opts)

    def run(self):
        if "db_path" not in self.opts:
            raise Exception("Database path is not specified.")
        else:
            ReportMode.generate_report(self.opts["db_path"])

    @staticmethod
    def generate_report(db_path, session_report=None, opts=None):
        """Prints out a status report to standard output. This function may be called from outside this class."""
        
        from database import Database

        with Database(db_path) as db:
            url_count = db.url_count
            fetched_url_count = db.fetched_url_count

            if session_report != None:
                print "-[ Spider Report: This session ]------------------------------------"
                print "  Number of fetch requests sent out: %d" % (opts["n_urls"])
                print "  Number of successful fetches: %s" % session_report['succeeded']
                print "  Live proxy hit ratio: %.02f%%" % (100.0 * session_report['succeeded'] / opts["n_urls"])
                print "  Sum of size of fetched documents: %d" % session_report['fetched_size']
                print "  Number of newly found URLs: %d" % session_report['new_urls_count']
                print

            print "-[ Spider Report: Overall summary ]------------------------------------"
            print "  Total number of URLs: %d" % url_count
            print "  Number of fetched URLs: %d" % fetched_url_count
            if url_count > 0:
                print "  Progress: %.02f%%" % (100.0 * fetched_url_count / url_count)


def parse_args(args):
    optlist, args = getopt.getopt(args, "u:n:t:d:smg", ("create-db", "single", "generate-report"))
    
    # default values
    opts = {}

    for o, a in optlist:
        if o == '-n':
            opts["n_urls"] = int(a)

        elif o == '-t':
            opts["n_proc"] = int(a)

        elif o == '-d':
            opts["db_path"] = a

        elif o in ("-u", "--url"):
            opts["url"] = a

        elif o == "--create-db":
            opts["run_mode"] = "create_db"

        elif o in ("-s", "--single"):
            opts["run_mode"] = "single"
            opts["n_urls"] = 1

        elif o in ("-m", "--multithreading"):
            opts["run_mode"] = "multithreading"

        elif o in ("-g", "--generate-report"):
            opts["run_mode"] = "generate_report"

    return opts

def validate_runtime_options(opts):
    if "run_mode" not in opts:
        return (False, "Run mode is not specified")

    elif (opts["run_mode"] == "create_db"):
        if ("db_path" not in opts):
            return (False, "SQLite3 database path must be supplied (-d)")
        else:
            return (True, "")

    elif (opts["run_mode"] == "single") and ("db_path" in opts) and ("url" in opts):
        return (True, "")

    elif (opts["run_mode"] == "multithreading"):
        if ("db_path" not in opts):
            return (False, "SQLite3 database path must be supplied (-d)")

        elif ("n_urls" not in opts):
            return (False, "Specify the number of URLs to fetch (-n)")

        elif ("n_proc" not in opts):
            return (False, "Specify the number of threads (-t)")

        else:
            return (True, "")

    return (False, "Unclassified error")

def main():
    opts = parse_args(sys.argv[1:])

    valid, message = validate_runtime_options(opts)

    if valid:
        run_mode = opts["run_mode"]

        if run_mode == "create_db":
            fend = CreateDBMode(opts)
            fend.run()
        
        elif run_mode == "single":
            fend = SingleMode(opts)
            fend.run()

        elif run_mode == "multithreading":
            fend = MultiThreadingMode(opts)
            fend.run()
        

        elif run_mode == "generate_report":
            fend = ReportMode(opts)
            fend.run()

    else:
        sys.stderr.write(message + "\n")

if __name__ == "__main__":
    main()
