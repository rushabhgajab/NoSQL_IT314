from mrjob.job import MRJob
import re
class MRImageCount(MRJob):
	def mapper(self, _, line):
		pattern = r"\[([0-9]{2})/([A-Za-z]{3})/([0-9]{4}):([0-9]{2}):([0-9]{2}):([0-9]{2}) [+-]([0-9]{4})\]"
		date = re.search(pattern, line)
		if date:
			date = date.groups()
			mm = date[1]
			yy = date[2]
			pattern = r"\" (\d{3}) (\d*) \""
			date = "{}-{}".format(yy, mm)
			size = re.search(pattern, line)
			if size:
				size = size.groups()[1]
				yield (date, (1, int(size)))

	def reducer(self, month, iterator):
		requests, size = map(sum, zip(*iterator))
		yield(month, (requests, size))
if __name__ == '__main__':
	MRImageCount.run()