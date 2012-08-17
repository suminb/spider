from spider import *
from database import Database
from proxy import *
from multiprocessing import Pool, Process, Lock, Semaphore, Queue, Manager

import re
import random

DB_URL = 'spider.db'
URL_PATTERN = r"http://messages.finance.yahoo.com/[A-Z][\/\w %=;&\.\-\+\?]*\/?"

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
        new_urls_count = 0

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
                new_urls_count = len(urls)
                print "Th:%d: Found %d URLs in %s." % (thread_seq, new_urls_count, url)
                db.insert_urls(urls)

            except urllib2.URLError as e:
                print 'URLError has been raised. Probably a proxy problem (%s).' % proxy
                print e

            except urllib2.HTTPError:
                print 'HTTP error has occoured. Deleting url %s' % url
                db.delete_url(url)

            except Exception as e:
                print 'Unclassified exception has occured: %s' % e

        # number of bytes of the fetched document
        fetched_size = len(document.content) if document != None and document.content != None else 0

        return {'succeeded':request_succeeded, 'new_urls_count':new_urls_count, 'fetched_size':fetched_size}

def fetch_unfetched_urls(limit):
    with Database(DB_URL) as db:
        curs = db.cursor
        curs.execute("SELECT url FROM document WHERE last_fetched IS NULL LIMIT ?", (limit,))
        
        return map(lambda u: u[0], curs.fetchall())

def fetch_urls(urls, urls_count, thread_seq):

    for url in urls:
        #print 'Th%d: (%d/%d)' % (thread_seq, len(urls), urls_count)
        fetch_url(url, thread_seq)


def reduce_report(row1, row2):
    return {'succeeded':row1['succeeded']+row2['succeeded'],
            'new_urls_count':row1['new_urls_count']+row2['new_urls_count'],
            'fetched_size':row1['fetched_size']+row2['fetched_size']}

def main():

    # number of processes
    n_proc = 32

    # number of urls to fetch
    n_urls = 1000

    urls = fetch_unfetched_urls(n_urls)

    pool = Pool(processes=n_proc)
    result = pool.map(fetch_url, urls)
    report = reduce(reduce_report, result)

    with Database(DB_URL) as db:
        url_count = db.url_count
        fetched_url_count = db.fetched_url_count

        print
        print "-[ Spider Report: This session ]------------------------------------"
        print "  Number of fetch requests sent out: %d" % (len(urls))
        print "  Number of successful fetches: %s" % report['succeeded']
        print "  Live proxy hit ratio: %.02f%%" % (100.0 * report['succeeded'] / len(urls))
        print "  Sum of size of fetched documents: %d" % report['fetched_size']
        print "  Number of newly found URLs: %d" % report['new_urls_count']
        print
        print "-[ Spider Report: Overall summary ]------------------------------------"
        print "  Total number of URLs: %d" % url_count
        print "  Number of fetched URLs: %d" % fetched_url_count
        print "  Progress: %.02f%%" % (100.0 * fetched_url_count / url_count)

if __name__ == '__main__':
    proxy_list = load_proxy_list("proxy_list.txt")
    main()
    #url = "http://messages.finance.yahoo.com/Business_%26_Finance/Investments/Stocks_%28A_to_Z%29/Stocks_J/threadview?bn=10073&tid=443633&mid=443634"
    #url ="http://messages.finance.yahoo.com/Stocks_%28A_to_Z%29/Stocks_A/threadview?m=tm&bn=1028&tid=1447176&mid=1447176&tof=35&rt=2&frt=2&off=1"
    #document = fetch_url(url)
