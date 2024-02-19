from mrjob.job import MRJob
from statistics import mean
import re

class WordCountAvg(MRJob):

    def mapper(self, _, line):
        text = re.sub(r'[^\w\s]', '', line.lower())
        yield None, sum(1 for word in text.split() if word != "blacks")

    def reducer(self, _, word_counts):
        yield "average", mean(word_counts)

if __name__ == '__main__':
    WordCountAvg.run()
