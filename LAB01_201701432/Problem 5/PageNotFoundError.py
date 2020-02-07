from mrjob.job import MRJob
import re
class MRImageCount(MRJob):
	def mapper(self, _, line):
		pattern = r"\" (\d{3}) (\d*) \""
		code = re.search(pattern, line)
		if code is not None:
			code = code.groups()[0]
			if code == "404":
				pattern = r"\"(http://[A-Za-z0-9-_./]*)\""
				url = re.search(pattern, line)
				pattern = r"([0-9]{2})/([A-Za-z]{3})/([0-9]{4}):([0-9]{2}):([0-9]{2}):([0-9]{2}) [+-]([0-9]{4})"
				date = re.search(pattern, line)
				if date is not None and url is not None:
					yield (date.group(), url.groups()[0])

if __name__ == '__main__':
	MRImageCount.run()