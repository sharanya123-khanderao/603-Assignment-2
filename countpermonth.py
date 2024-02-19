from mrjob.job import MRJob
import re

class CountPerMonth(MRJob):

    def mapper(self, _, line):
        # Use regular expression to extract date
        date_match = re.search(r'\d{4}-\d{2}', line)
        if date_match:
            year_month = date_match.group(0)
            yield year_month, 1

    def reducer(self, month, counts):
        # Sum up the counts for each month
        yield month, sum(counts)

if __name__ == '__main__':
    CountPerMonth.run()
