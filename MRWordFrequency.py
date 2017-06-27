from mrjob.job import MRJob


class MRWordFrequencyCount(MRJob):

    age1 = 0
    age2 = 0
    gender = ""

    def setage1(self, age1):
        self.age1 = age1

    def setage2(self, age2):
        self.age2 = age2

    def setgender(self, gender):
        self.gender = gender

    def mapper(self, _, line):
        yield "chars", len(line)
        yield "words", len(line.split())
        yield "lines", 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRWordFrequencyCount.run()