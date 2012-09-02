"""TODO: Separation of key (URL) and value (Document) storages"""

class DataStore:
	"""Defines a common interface for data storage."""

	def list(self, key_prefix):
		raise Exception("Cannot call an abstract function")

	def has(self, key):
		"""Indicates wheter this data storage has a value associated with the key."""
		raise Exception("Cannot call an abstract function")

	def load(self, key):
		raise Exception("Cannot call an abstract function")

	def store(self, key, value):
		"""
		key -- URL of Document
		value -- Document instance
		"""
		raise Exception("Cannot call an abstract function")

	def delete(self, key):
		raise Exception("Cannot call an abstract function")

	def stat(self, cached=True):
		raise Exception("Cannot call an abstract function")


class SQLiteStorage(DataStore):
	def load(self, key):
		pass

	def store(self, key, value):
		pass


class FilesystemStorage(DataStore):
	def __init__(self, config={}):
		# TODO: Make this configurable
		config["dir"] = "/tmp"

		self.config = config


	def load(self, key):
		pass

	def store(self, key, value):
		pass