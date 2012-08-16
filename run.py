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
	task = FetchTask(url)
	content = task.run(proxy, db)
	doc = Document(content)
	
	file_name = 'docs/%s.html' % hash_url(url)
	with open(file_name, 'w') as f:
		f.write(content.encode('utf-8'))

# This is about 2.5 times faster than the non-parallel method
#pool = Pool(processes=4)
#print pool.map(f, urls)

#print map(f, urls)

#doc = Document(open('sample.html').read())
#print doc.extract_urls()

fetch("http://messages.finance.yahoo.com/Business_%26_Finance/Investments/Stocks_%28A_to_Z%29/Stocks_L/threadview?bn=76474&tid=35875&mid=35912")
