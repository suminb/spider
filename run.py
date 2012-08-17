from spider import *
from database import Database
from proxy import *
from multiprocessing import Pool, Process, Lock, Semaphore, Queue, Manager

import re
import random

DB_URL = 'spider.db'
URL_PATTERN = r"http://messages.finance.yahoo.com/Business_%26_Finance/Investments/Stocks_%28A_to_Z%29/Stocks_J[\/\w %=;&\.\-\+\?]*\/?"

#proxy = Proxy('http', '79.170.50.25', 80)

proxy_list = None

def load_proxy_list(file_name):
    with open(file_name) as f:
        return re.findall(r"(https?):\/\/([0-9a-z\.]+):(\d+)", f.read())

def fetch_url(url, thread_seq=0):
    with Database(DB_URL) as db:
        document = db.fetch_document(url)
        has_url = (document != None)

        request_succeeded = 0

        pxy = random.choice(proxy_list)
        proxy = Proxy(pxy[0], pxy[1], pxy[2])

        if document == None or document.content == None:
            print "Th%d: Fetching %s via %s" % (thread_seq, url, proxy)
            task = FetchTask(url)
            try:
                document = task.run(proxy, db)
                request_succeeded = 1

                if has_url:
                    db.update_document(document)
                else:
                    db.insert_document(document)

                urls = document.extract_urls(URL_PATTERN)
                print "Th:%d: Found %d URLs in %s." % (thread_seq, len(urls), url)
                db.insert_urls(urls)

            except urllib2.URLError as e:
                print 'URLError has been raised. Probably a proxy problem (%s).' % proxy
                print e

            except urllib2.HTTPError:
                print 'HTTP error has occoured. Deleting url %s' % url
                db.delete_url(url)

        return (request_succeeded, len(document.content))

def fetch_unfetched_urls(limit):
    with Database(DB_URL) as db:
        curs = db.cursor
        curs.execute("SELECT url FROM document WHERE last_fetched IS NULL LIMIT ?", (limit,))
        
        return map(lambda u: u[0], curs.fetchall())

def fetch_urls(urls, urls_count, thread_seq):

    for url in urls:
        #print 'Th%d: (%d/%d)' % (thread_seq, len(urls), urls_count)
        fetch_url(url, thread_seq)


# This is about 2.5 times faster than the non-parallel method
#pool = Pool(processes=4)
#print pool.map(f, urls)

#print map(f, urls)

#doc = Document(open('sample.html').read())
#print doc.extract_urls()

def main():

    # number of processes
    n_proc = 32

    # number of urls per process
    n_urls_pp = 50

    urls = fetch_unfetched_urls(n_proc * n_urls_pp)
    
    p = [None,] * n_proc
    for i in range(n_proc):
        partial_urls = urls[i*n_urls_pp:(i+1)*n_urls_pp]
        p[i] = Process(target=fetch_urls, args=(partial_urls, len(partial_urls), i))
        p[i].start()
    for i in range(n_proc):
        p[i].join()

    with Database(DB_URL) as db:
        url_count = db.url_count
        fetched_url_count = db.fetched_url_count

        print "-[ Spider Report ]------------------------------------"
        print "Total number of URLs: %d" % url_count
        print "Number of fetched URLs: %d" % fetched_url_count
        print "Progress: %.02f%%" % (100.0 * fetched_url_count / url_count)

if __name__ == '__main__':
    proxy_list = load_proxy_list("proxy_list.txt")
    main()
    #url = "http://messages.finance.yahoo.com/Business_%26_Finance/Investments/Stocks_%28A_to_Z%29/Stocks_J/threadview?bn=10073&tid=443633&mid=443634"
    #document = fetch_url(url)
