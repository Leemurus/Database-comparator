from collections import namedtuple
from pprint import PrettyPrinter


class Analyze:
    def __init__(self, path):
        self._file_csv = open(path, 'r')
        self._databases = []

        for db in self._file_csv.readline().rstrip().split(',')[2:]:
            db_name, db_doc = db.split(';')
            self._databases.append(namedtuple(db_name, ['empty', 'doc']))
            self._databases[-1].empty = 0
            self._databases[-1].doc = db_doc.replace('|', ',')

    def get_total_statistics(self):
        total = 0

        for line in self._file_csv:
            total += 1
            cities = list(line.rstrip().split(',')[2:])

            for index in range(len(cities)):
                if not cities[index]:
                    self._databases[index].empty += 1

        statistic = []
        for db in self._databases:
            statistic.append({
                'db_name': db.__name__,
                'description': db.doc,
                'empty fields': str(round(db.empty / total * 100, 2)) + '%',
                'with answer': str(round((total - db.empty) / total * 100, 2)) + '%'
            })

        return statistic, total


def analyze():
    analyze = Analyze('res.csv')
    PrettyPrinter().pprint(analyze.get_total_statistics())


if __name__ == '__main__':
    analyze()
