from spider import *
from proxy import *
from multiprocessing import Pool, Process, Lock, Semaphore, Queue

import random

DB_URL = 'spider.db'
URL_PATTERN = r"http://messages.finance.yahoo.com/Stocks[\/\w %=;&\.\-\+\?]*\/?"

#proxy = Proxy('http', '79.170.50.25', 80)

proxy_list = (
    ('http', '46.21.155.222', 3128),
    ('http', '189.84.112.118', 3128),
    ('http', '190.145.45.246', 3128),
    ('http', '190.121.143.242', 8080),
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
                print "Th:%d: Found %d URLs in %s." % (thread_seq, len(urls), url[:40])
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

def fetch_urls(urls, urls_count, thread_seq, lock, queue):
    #for url in urls:

    print 'Th%d: %s' % (thread_seq, id(urls))

    while len(urls) > 0:
    #while not queue.empty():
        # Python lists are thread-safe themselves
        
        lock.acquire()
        url = urls.pop(thread_seq)
        #url = queue.get()
        print 'Th%d: (%d/%d)' % (thread_seq, len(urls), urls_count)
    

        fetch_url(url, thread_seq)

        lock.release()

# This is about 2.5 times faster than the non-parallel method
#pool = Pool(processes=4)
#print pool.map(f, urls)

#print map(f, urls)

#doc = Document(open('sample.html').read())
#print doc.extract_urls()

def main():

    # number of processes
    n = 4

    lock = Lock()

    urls = fetch_unfetched_urls(15*n)

    queue = Queue()
    for url in urls:
        queue.put(url)
    
    p = [None,]*n
    for i in range(n):
        #p[i] = Process(target=fetch_urls, args=(urls[i*n:(i+1)*n], i))

        # http://docs.python.org/library/multiprocessing.html#sharing-state-between-processes
        p[i] = Process(target=fetch_urls, args=(urls, len(urls), i, lock, queue))
        p[i].start()
    for i in range(n):
        p[i].join()


    # for document in documents:
    #   has_url = db.has_url(document.url)

    #   if has_url:
    #       db.update_document(document)
    #   else:
    #       db.insert_document(document)

if __name__ == '__main__':
    main()
    #url = "http://messages.finance.yahoo.com/Stocks_%28A_to_Z%29/Stocks_L/threadview?m=tm&bn=76474&tid=35845&mid=35874&tof=9&frt=2"
    #url = "http://messages.finance.yahoo.com/Stocks_%28A_to_Z%29/Stocks_L/threadview?m=mm&bn=76474&tid=35875&mid=35935&tof=-1&rt=2&frt=2&off=1"
    #document = fetch_url(url)

    # if db.has_url(url):
    #   db.update_document(document)
    # else:
    #   db.insert_document(document)

#   urls = document.extract_urls(r"http://messages.finance.yahoo.com/[\/\w %=;&\.\-\+\?]*\/?")
