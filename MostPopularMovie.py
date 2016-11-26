"""
To run the job:
python MostPopularMovie.py --items=ml-100k/u.item ml-100k/u.data

Input :- ml-100k folder
Output :- On the console

"""
from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):

    def configure_options(self):
        super(MostPopularMovie, self).configure_options()
        self.add_file_option('--items', help='Path to u.item')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_count_ratings),
            MRStep(mapper = self.mapper_passthrough,
                   reducer = self.reducer_find_max)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_init(self):
        self.movieNames = {}

        file = open("u.item")
        for line in file:
            fields = line.split('|')
            self.movieNames[fields[0]] = fields[1].decode('utf-8', 'ignore')

    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), self.movieNames[key])

    def mapper_passthrough(self, key, value):
        yield key, value

    def reducer_find_max(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MostPopularMovie.run()
