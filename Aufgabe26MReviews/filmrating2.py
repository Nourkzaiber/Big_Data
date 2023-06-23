from mrjob.job import MRJob
from mrjob.step import MRStep
import time

class FilmRating(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_rating,
                   combiner=self.combiner_count_rating,
                   reducer=self.reducer_count_rating),

            MRStep(reducer=self.reducer_Top_rating),

        ]

    def mapper_get_rating(self, _, line):
        #(userID, movieID, rating, timestamp) = line.split(',')
        #yield movieID, float(rating)
        if not line.startswith("userID"):  # Überprüfung auf die Kopfzeile
            (userID, movieID, rating, timestamp) = line.split(',')
            try:
                rating = float(rating)
                yield movieID, rating
            except ValueError:
                pass  # Ungültige Bewertungen überspringen

    def combiner_count_rating(self, movieID, ratings):
        yield movieID, sum(ratings)

    def reducer_count_rating(self, movieID, ratings):
        yield None, (sum(ratings), movieID)

    def reducer_Top_rating(self, movieID, rating_movie_pairs):
         #yield (max(rating_movie_pairs))
         top_film = sorted(rating_movie_pairs, reverse=True)[:10]
         for movieID, ratings in top_film:
             yield movieID, ratings



if __name__ == '__main__':

    start = time.time()

    FilmRating.run()
    ende = time.time()

    dauer_ms = int((ende - start) * 1000)
    print(f" Dauer: {dauer_ms:,d} ms")

   #  Dauer: 3,858 ms für 660 kb
   #  Dauer: 302,582 ms  für 174.069 kb
