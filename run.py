from spider import *
from proxy import *
from multiprocessing import Pool

import hashlib


proxy = Proxy('http', '79.170.50.25', 80)
db = Database('spider.db')
db.connect()

urls = ('http://docs.python.org/library/multiprocessing',
	'http://stackoverflow.com/questions/5620263/using-an-http-proxy-python',
	'https://github.com/suminb/spider',
	'http://news.cnet.com/',
	'http://www.python.org/dev/peps/pep-0257/')

def hash_url(url):
	return hashlib.sha1(url).hexdigest()

def fetch(url, has_url=False):
	print "Fetching %s... via %s" % (url[:40], proxy)

	document = None
	if has_url:
		document = db.fetch_document(url)

	if document == None or document.content == None:
		task = FetchTask(url)
		document = task.run(proxy, db)

	return document

def fetch_unfetched_urls(db, limit):
	curs = db.cursor
	curs.execute("SELECT url FROM document WHERE last_fetched IS NULL LIMIT ?", (limit,))
	return map(lambda u: u[0], curs.fetchall())


# This is about 2.5 times faster than the non-parallel method
#pool = Pool(processes=4)
#print pool.map(f, urls)

#print map(f, urls)

#doc = Document(open('sample.html').read())
#print doc.extract_urls()

def main():
	urls = fetch_unfetched_urls(db, 4)
	pool = Pool(processes=4)
	documents = pool.map(fetch, urls)

	for document in documents:
		has_url = db.has_url(document.url)

		if has_url:
			db.update_document(document)
		else:
			db.insert_document(document)

if __name__ == '__main__':
	main()
	# url = "http://messages.finance.yahoo.com/Stocks_%28A_to_Z%29/Stocks_L/threadview?m=tm&bn=76474&tid=35845&mid=35874&tof=9&frt=2"
	# document = fetch(url)

	# if db.has_url(url):
	# 	db.update_document(document)
	# else:
	# 	db.insert_document(document)

# 	urls = document.extract_urls(r"http://messages.finance.yahoo.com/[\/\w %=;&\.\-\+\?]*\/?")
