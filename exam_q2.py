from mrjob.job import MRJob
from mrjob.step import MRStep

class TagsBreakdown(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_users_tags,
                   reducer=self.reducer_tags_analysis)
        ]

    def mapper_users_tags(self, _, line):
        try:
            userID, movieID, tag, timestamp = line.strip().split('\t')

            yield f"[USER_TAG_COUNT]\t{userID}", 1

        except Exception:
            pass

    def reducer_tags_analysis(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    TagsBreakdown.run()
