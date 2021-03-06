from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env

class MRJobCount(MRJob):

    def mapper(self, _, line):
        lower = int(jobconf_from_env('my.job.lower_height'))
        upper = int(jobconf_from_env('my.job.upper_height'))
        zipc = int(jobconf_from_env('my.job.zip_code'))

        if lower > upper:
            lower, upper = upper, lower

        (ZipCode,Centimeters) = line.split(',')

        if lower <= int(Centimeters) <= upper:
            if int(ZipCode) == zipc:
                yield ZipCode, 1

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == '__main__':
    MRJobCount.run()