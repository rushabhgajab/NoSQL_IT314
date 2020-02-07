from mrjob.job import MRJob
class MRMaxTemp(MRJob):
	def mapper(self, _, line):
		MISSING = 9999
		good_quality = set("01459")
		year = line[15:19]
		temperature = int(line[87:92])
		quality = line[92:93]
		if quality in good_quality and abs(temperature) != MISSING:
			yield(year, temperature)
	def reducer(self, year, temperatures):
		yield(year, max(temperatures))
if __name__ == '__main__':
	MRMaxTemp.run()
