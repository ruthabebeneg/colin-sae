from mrjob.job import MRJob
from mrjob.step import MRStep

class tagsmemeuser(MRJob):
    def steps(self):

        return [MRStep(mapper=self.mapper,

                       reducer=self.reducer)]
    def mapper(self, _, line):
        try:

            userID, movieID, tag, timestamp = line.strip().split(",", 3)

            key = (movieID, userID)

            yield key, 1

        except:

            pass

    def reducer(self, key, counts):

        yield key, sum(counts)

if __name__ == '__main__':

    tagsmemeuser.run()
