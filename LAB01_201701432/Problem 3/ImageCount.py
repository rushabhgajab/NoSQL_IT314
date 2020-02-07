from mrjob.job import MRJob
import re
class MRImageCount(MRJob):
	def mapper(self, _, line):
		match_ext = {"png", "jpg", "gif"}
		request = re.search(r"GET[A-Za-z_\/. 0-9]*HTTP",line)
		if request:
			for i in match_ext:
				if i in request.group().lower():
					yield(i, 1)
	def reducer(self, ext, occurances):
		yield(ext, sum(occurances))
if __name__ == '__main__':
	MRImageCount.run()