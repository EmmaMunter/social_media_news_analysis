from collections.abc import Mapping

# This class loads the results of the source_callable on demand.
# It's essentially just a cache.
# But, note that other code uses it for "caching" things like currently loaded models too!
class AutoLoader(Mapping):
	def __init__(self, source_callable):
		self._inner = {}
		self._source_callable = source_callable
	def __getitem__(self, key):
		try:
			return self._inner.__getitem__(key)
		except KeyError:
			self._inner[key] = self._source_callable(key)
			return self._inner.__getitem__(key)
	def __iter__(self):
		return self._inner.__iter__()
	def __len__(self):
		return self._inner.__len__()
