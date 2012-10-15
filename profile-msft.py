
# Specify the number of URLs to fetch (int or "Auto")
URLS = "Auto"

# Specify the number of threads (int or "Auto")
THREADS = 96

# Path of database file
DB_URI = "yahoo-msft.db"

# Path of proxy IP:port list file
# This will be replaced with something better in the future, but we'll stick with this for now.
PROXY_LIST_URI = "proxy_list.txt"

# Entry points
# If no URL exists in the database, Spider will try to fetch these URLs first
ENTRY_POINTS = (
	"http://finance.yahoo.com/mb/forumview/?&bn=30ec85b4-d1ce-32fe-a71c-f56c94fe758b",
	"http://finance.yahoo.com/mbview/threadview/?&bn=30ec85b4-d1ce-32fe-a71c-f56c94fe758b&tid=1343911836000-0fbebf76-5ff6-3a80-a415-26faca45238a&stb=n&lv=e&la=ml",
)

# URLs that match there patterns will be fetched
URL_PATTERNS = (
	r"https?://finance.yahoo.com/[\/\w %=;&_\.\-\+\?]*bn=30ec85b4-d1ce-32fe-a71c-f56c94fe758b[\/\w %=;&_\.\-\+]*\/?",
)


def generate_key(url):
	return url # 30ec85b4-d1ce-32fe-a71c-f56c94fe758b:${tid}

def process_content(content):
	import json

	page = Page(content)
	return json.dumps({"title":page.title, "content":page.content, "timestamp":page.timestamp})

class Page:
	def __init__(self, content):
		from bs4 import BeautifulSoup

		self.soup = BeautifulSoup(content)

	@property
	def title(self):
		return self.soup.find("div", {"class": "mb-title"}).find("h4").string

	@property
	def content(self):
		return self.soup.find("p", {"class": "mb-text-full"}).string

	@property
	def timestamp(self):
		return self.soup.find("span", {"class": "mb-timestamp"}).abbr["title"]

