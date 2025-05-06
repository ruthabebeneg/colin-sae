from mrjob.job import MRJob
from mrjob.step import MRStep

class tagbd(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_users_tags,
                reducer=self.reducer_tags_analysis
            )
        ]

    def mapper_users_tags(self, _, line):
        try:
            userID, movieID, tag, timestamp = line.strip().split('\t')
            yield userID, 1 
        except ValueError:
            pass

    def reducer_tags_analysis(self, userID, counts):
        yield userID, sum(counts)

if __name__ == '__main__':
    tagbd.run()
