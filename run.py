from spider import *
from proxy import *
from multiprocessing import Pool, Process

import hashlib

DB_URL = 'spider.db'
URL_PATTERN = r"http://messages.finance.yahoo.com/Stocks[\/\w %=;&\.\-\+\?]*\/?"

proxy = Proxy('http', '79.170.50.25', 80)
proxy = None

urls = ('http://docs.python.org/library/multiprocessing',
	'http://stackoverflow.com/questions/5620263/using-an-http-proxy-python',
	'https://github.com/suminb/spider',
	'http://news.cnet.com/',
	'http://www.python.org/dev/peps/pep-0257/')

def hash_url(url):
	return hashlib.sha1(url).hexdigest()

def fetch_url(url, thread_seq=0):
	with Database(DB_URL) as db:
		document = db.fetch_document(url)
		has_url = (document != None)

		if document == None or document.content == None:
			print "Th%d: Fetching %s... via %s" % (thread_seq, url[:40], proxy)
			task = FetchTask(url)
			try:
				document = task.run(proxy, db)
			except urllib2.HTTPError:
				print 'HTTP error has occoured. Deleting url %s' % url
				db.delete_url(url)

			if has_url:
				db.update_document(document)
			else:
				db.insert_document(document)

			urls = document.extract_urls(URL_PATTERN)
			print "Th:%d: Found %d URLs in %s." % (thread_seq, len(urls), url[:40])
			db.insert_urls(urls)

		return document

def fetch_unfetched_urls(limit):
	with Database(DB_URL) as db:
		curs = db.cursor
		curs.execute("SELECT url FROM document WHERE last_fetched IS NULL LIMIT ?", (limit,))
		
		return map(lambda u: u[0], curs.fetchall())

def process_urls(urls, thread_seq):
	for url in urls:
		fetch_url(url, thread_seq)

# This is about 2.5 times faster than the non-parallel method
#pool = Pool(processes=4)
#print pool.map(f, urls)

#print map(f, urls)

#doc = Document(open('sample.html').read())
#print doc.extract_urls()

def main():

	# number of processes
	n = 8

	urls = fetch_unfetched_urls(10*n)
	
	p = [None,]*n
	for i in range(n):
		p[i] = Process(target=process_urls, args=(urls[i*n:(i+1)*n], i))
		p[i].start()
	for i in range(n):
		p[i].join()


	# for document in documents:
	# 	has_url = db.has_url(document.url)

	# 	if has_url:
	# 		db.update_document(document)
	# 	else:
	# 		db.insert_document(document)

if __name__ == '__main__':
	main()
	#url = "http://messages.finance.yahoo.com/Stocks_%28A_to_Z%29/Stocks_L/threadview?m=tm&bn=76474&tid=35845&mid=35874&tof=9&frt=2"
	#url = "http://messages.finance.yahoo.com/Stocks_%28A_to_Z%29/Stocks_L/threadview?m=mm&bn=76474&tid=35875&mid=35935&tof=-1&rt=2&frt=2&off=1"
	#document = fetch_url(url)

	# if db.has_url(url):
	# 	db.update_document(document)
	# else:
	# 	db.insert_document(document)

# 	urls = document.extract_urls(r"http://messages.finance.yahoo.com/[\/\w %=;&\.\-\+\?]*\/?")
