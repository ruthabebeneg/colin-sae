from mrjob.job import MRJob
from mrjob.step import MRStep

class TagsBreakdown(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_film_tag,
                   reducer=self.reducer_tags_analysis)
        ]

    def mapper_film_tag(self, _, line):
        try:
            userID, movieID, tag, timestamp = line.strip().split('\t')

            # 1. Nombre de tags par film
            yield ("film_tags",movieID),1

        except Exception:
            pass

    def reducer_tags_analysis(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    TagsBreakdown.run()


# comment executer : 
# python tags_breakdown.py tags.csv > resultats_film_tag.txt
# les resultats sont dans resultats.txt