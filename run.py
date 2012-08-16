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

def fetch(url):
	print "Fetching %s... via %s" % (url[:40], proxy)

	task = FetchTask(url)
	content = task.run(proxy, db)

	doc = Document(content)
	urls = doc.extract_urls(r"http://messages.finance.yahoo.com/[\/\w %=;&\.\-\+\?]*\/?")

	db.insert_urls(urls)

def fetch_unfetched_urls(db):
	curs = db.cursor
	curs.execute("SELECT url FROM document WHERE last_fetched IS NULL LIMIT 100")
	return map(lambda u: u[0], curs.fetchall())

# This is about 2.5 times faster than the non-parallel method
#pool = Pool(processes=4)
#print pool.map(f, urls)

#print map(f, urls)

#doc = Document(open('sample.html').read())
#print doc.extract_urls()

def main():
	urls = fetch_unfetched_urls(db)
	for url in urls:
		fetch(url)

if __name__ == '__main__':
	main()
