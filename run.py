from spider import *
from database import Database
from proxy import *
from multiprocessing.pool import ThreadPool

import re
import random
import thread
import threading
import curses

DB_URL = 'spider.db'
URL_PATTERNS = (
    r"http://messages.finance.yahoo.com/[A-Z][\/\w %=;&\.\-\+\?]*\/?",
    r"http://messages.finance.yahoo.com/search[\/\w %=;&\.\-\+\?]*\/?",
)

def fetch_unfetched_urls(limit):
    with Database(DB_URL) as db:
        curs = db.cursor
        curs.execute("SELECT url FROM document WHERE last_fetched IS NULL LIMIT ?", (limit,))
        
        return map(lambda u: u[0], curs.fetchall())

#
# Shared resources
#

# number of processes
n_proc = 64

# number of urls to fetch
n_urls = 10000

unfetched_urls = fetch_unfetched_urls(n_urls)

proxy_list = None

status = {'processed_urls_count':0}
thread_status = {}
lock = threading.Lock()
screen = curses.initscr()


def load_proxy_list(file_name):
    with open(file_name) as f:
        return re.findall(r"(https?):\/\/([0-9a-z\.]+):(\d+)", f.read())

def truncate_middle(str, max_length):
    str_len = len(str)
    if str_len > max_length:
        m = (max_length / 2) 
        return "%s...%s" % (str[:m-2], str[str_len-m+1:])
    else:
        return str

def fetch_url(url, thread_seq=0):
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

    with Database(DB_URL) as db:
        document = db.fetch_document(url)
        has_url = (document != None)

        request_succeeded = 0
        new_urls_count = 0        

        if document == None or document.content == None:
            #print "Th%d: Fetching %s via %s" % (thread_seq, url, proxy)
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
                #print "Th:%d: Found %d URLs in %s." % (thread_seq, new_urls_count, url)

            except urllib2.URLError as e:
                #print 'URLError has been raised. Probably a proxy problem (%s).' % proxy
                #print e
                thread_status[tid]['message'] = "URLError has been raised. Probably a proxy problem (%s)" % proxy

            except urllib2.HTTPError:
                #print 'HTTP error has occoured. Deleting url %s' % url
                thread_status[tid]['message'] = "HTTP error has occoured. Deleting url %s" % url
                db.delete_url(url)

            except Exception as e:
                #print 'Unclassified exception has occured: %s' % e
                thread_status[tid]['message'] = "Unclassified exception has occured: %s" % e

        # number of bytes of the fetched document
        fetched_size = len(document.content) if document != None and document.content != None else 0

        lock.acquire()
        status['processed_urls_count'] += 1
        thread_status[tid]['new_urls_count'] = new_urls_count

        screen.erase()

        # screen width and height
        height, width = screen.getmaxyx()
        
        for key in thread_status.keys()[:height-3]:
            screen.addstr("[%x] " % key)
            if thread_status[key]['message'] == None:
                screen.addstr("Fetching %s via %s\n" % (truncate_middle(thread_status[key]['url'], 50), thread_status[key]['proxy']))
            else:
                screen.addstr(thread_status[key]['message'] + "\n")

        if height - 3 < n_proc:
            screen.addstr("... and %d more threads are running ...\n" % (n_proc - (height - 3)))

        screen.addstr("(%d/%d)" % (status['processed_urls_count'], len(unfetched_urls)))

        screen.refresh()
    
        lock.release()

        return {'succeeded':request_succeeded, 'new_urls_count':new_urls_count, 'fetched_size':fetched_size}

def reduce_report(row1, row2):
    return {'succeeded':row1['succeeded']+row2['succeeded'],
            'new_urls_count':row1['new_urls_count']+row2['new_urls_count'],
            'fetched_size':row1['fetched_size']+row2['fetched_size']}

def main():
    # stops curses from outputting key presses from the user onto the screen
    curses.noecho() 

    # removes the cursor from the screen
    curses.curs_set(0)

    # sets the mode which the screen uses when capturing key presses 
    screen.keypad(1)

    pool = ThreadPool(n_proc)
    result = pool.map(fetch_url, unfetched_urls)
    report = reduce(reduce_report, result)

    with Database(DB_URL) as db:
        url_count = db.url_count
        fetched_url_count = db.fetched_url_count

        curses.endwin()

        print
        print "-[ Spider Report: This session ]------------------------------------"
        print "  Number of fetch requests sent out: %d" % (len(unfetched_urls))
        print "  Number of successful fetches: %s" % report['succeeded']
        print "  Live proxy hit ratio: %.02f%%" % (100.0 * report['succeeded'] / len(unfetched_urls))
        print "  Sum of size of fetched documents: %d" % report['fetched_size']
        print "  Number of newly found URLs: %d" % report['new_urls_count']
        print
        print "-[ Spider Report: Overall summary ]------------------------------------"
        print "  Total number of URLs: %d" % url_count
        print "  Number of fetched URLs: %d" % fetched_url_count
        print "  Progress: %.02f%%" % (100.0 * fetched_url_count / url_count)

if __name__ == '__main__':
    proxy_list = load_proxy_list("proxy_list.txt")
    try:
        main()
    except:
        curses.endwin()
        import traceback
        traceback.print_exc()

    #url = "http://messages.finance.yahoo.com/Business_%26_Finance/Investments/Stocks_%28A_to_Z%29/Stocks_J/threadview?bn=10073&tid=443633&mid=443634"
    #url ="http://messages.finance.yahoo.com/Stocks_%28A_to_Z%29/Stocks_A/threadview?m=tm&bn=1028&tid=1447176&mid=1447176&tof=35&rt=2&frt=2&off=1"
    #fetch_url("http://messages.finance.yahoo.com/Stocks_%28A_to_Z%29/Stocks_M/threadview?m=ts&bn=12004&tid=1800636&mid=1800636&tof=1&frt=2")
