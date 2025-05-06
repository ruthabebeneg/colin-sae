from mrjob.job import MRJob
from mrjob.step import MRStep

class TagsBreakdown(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_user_film_tag_count,
                   reducer=self.reducer_tags_analysis)
        ]

    def mapper_user_film_tag_count(self, _, line):
        try:
            userID, movieID, tag, timestamp = line.strip().split('\t')

            yield f"[USER_FILM_TAG_COUNT]\t{movieID}|{userID}", 1

        except Exception:
            pass

    def reducer_tags_analysis(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    TagsBreakdown.run()
