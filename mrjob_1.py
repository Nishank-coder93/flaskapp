from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env

class MRJobCount(MRJob):

    def mapper(self, _, line):
        lower = int(jobconf_from_env('my.job.lower_age'))
        upper = int(jobconf_from_env('my.job.upper_age'))
        if lower > upper:
            lower, upper = upper, lower

        (Gender,Age,Kilograms) = line.split(',')

        if lower <= int(Age) <= upper:
            yield Gender, 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRJobCount.run()