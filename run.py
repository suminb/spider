from spider import *
from multiprocessing import Pool

proxy = Proxy('http', '79.170.50.25', 80)

urls = ('http://docs.python.org/library/multiprocessing',
	'http://stackoverflow.com/questions/5620263/using-an-http-proxy-python',
	'https://github.com/suminb/spider',
	'http://news.cnet.com/')

def f(url):
	task = FetchTask(url)
	content = task.run(proxy)

	return len(content)

# This is about 2.5 times faster than the non-parallel method
pool = Pool(processes=4)
print pool.map(f, urls)

#print map(f, urls)