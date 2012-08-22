
class Frontend:
	def __init__(self, opts):
		self.opts = opts

		# shared across multiple threads
		self.shared = {}

	def run(self):
		raise Exception("Not implemented")

	def prepare_curses(self):
		pass

	def cleanup_curses(self):
		pass


class SingleMode(Frontend):

	def run(self):
		report = fetch_url(self.opts["url"])

		if self.opts["generate_report"]:
            generate_report(report)

class MultiThreadingMode(Frontend):
	def run(self):
		pass


class CreateDBMode(Frontend):
	def run(self):
		create_sqlite3_db(self.opts["db_path"])


class ReportMode(Frontend):
	def run(self):
		pass



