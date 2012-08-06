from spider import *

proxy = Proxy('http', '79.170.50.25', 80)
task = FetchTask('http://www.facebook.com/suminb')
task.run(proxy)