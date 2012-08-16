from spider import *
from proxy import *
from multiprocessing import Pool, Process, Lock, Semaphore, Queue, Manager

import random

DB_URL = 'spider.db'
URL_PATTERN = r"http://messages.finance.yahoo.com/Business_%26_Finance/Investments/Stocks_%28A_to_Z%29/Stocks_J[\/\w %=;&\.\-\+\?]*\/?"

#proxy = Proxy('http', '79.170.50.25', 80)

proxy_list = (
    ('http', '46.21.155.222', 3128),
    ('http', '189.84.112.118', 3128),
    ('http', '190.145.45.246', 3128),
    ('http', '177.38.178.25', 3128),
    ('http', '190.184.215.7', 8080),
    ('http', '106.187.35.52', 8080),
    ('http', '180.183.184.222', 3128),
    ('http', '46.39.225.33', 3128),
    ('http', '110.171.33.200', 3128),

    #('https', '200.12.49.40', 8080),
)

def hash_url(url):
    return hashlib.sha1(url).hexdigest()

def fetch_url(url, thread_seq=0):
    with Database(DB_URL) as db:
        document = db.fetch_document(url)
        has_url = (document != None)

        pxy = random.choice(proxy_list)
        proxy = Proxy(pxy[0], pxy[1], pxy[2])

        if document == None or document.content == None:
            print "Th%d: Fetching %s via %s" % (thread_seq, url, proxy)
            task = FetchTask(url)
            try:
                document = task.run(proxy, db)

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

        return document

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
    n = 6

    urls = fetch_unfetched_urls(100*n)
    
    p = [None,]*n
    for i in range(n):
        partial_urls = urls[i*n:(i+1)*n]
        p[i] = Process(target=fetch_urls, args=(partial_urls, len(partial_urls), i))
        p[i].start()
    for i in range(n):
        p[i].join()

if __name__ == '__main__':
    main()
    #url = "http://messages.finance.yahoo.com/Business_%26_Finance/Investments/Stocks_%28A_to_Z%29/Stocks_J/threadview?bn=10073&tid=443633&mid=443634"
    #document = fetch_url(url)

    # if db.has_url(url):
    #   db.update_document(document)
    # else:
    #   db.insert_document(document)

#   urls = document.extract_urls(r"http://messages.finance.yahoo.com/[\/\w %=;&\.\-\+\?]*\/?")
