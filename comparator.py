from databases.db_ip import DBIP
from databases.geo_lite import GeoLite
from databases.ip2location import IP2Location
from databases.pysyge import SypexGeo

from databases.database import Database
from multiprocessing import Process, Manager


class Comparator:
    def __init__(self, path, res_file):
        self._resource_file = open(path, 'r')
        self._res_file = open(res_file, 'w')

    def _write_header(self, databases):
        self._res_file.write('IP,test result')
        for database in databases:
            self._res_file.write(',' + type(database).__name__ + ';' + database.__doc__.replace(',', '|'))
        self._res_file.write('\n')
        self._res_file.flush()

    @staticmethod
    def calc_geolocations(databases, ips):
        processes = []
        return_dict = Manager().dict()
        for proc_num, database in enumerate(databases):
            process = Process(target=database.get_data, args=(ips, proc_num, return_dict))
            processes.append(process)
            process.start()

        for process in processes:
            process.join()

        results = []
        for proc_num in sorted(return_dict.keys()):
            results.append(return_dict[proc_num])

        return results

    def _get_ips_from_resource_file(self):
        self._resource_file.seek(0, 0)
        ips = list(Database.ip2int(line.split(',')[0]) for line in self._resource_file)
        self._resource_file.seek(0, 0)
        return ips

    def compare(self, databases):
        self._write_header(databases)

        ips = self._get_ips_from_resource_file()
        geolocations = self.calc_geolocations(databases, ips)

        for index, line in enumerate(self._resource_file):
            self._res_file.write(Database.int2ip(ips[index]))  # ip
            self._res_file.write(',' + line.split(',')[1].rstrip())  # result from test

            for geolocation_answers in geolocations:
                self._res_file.write(',' + geolocation_answers[index])

            self._res_file.write('\n')
            self._res_file.flush()


def compare():
    databases = (
        IP2Location('data/IP2LOCATION-LITE-DB5.CSV'),
        DBIP('data/dbip-city-lite-2020-07.csv'),
        SypexGeo('data/SxGeoCity.dat'),
        GeoLite('data/GeoLite2-City.mmdb')
    )

    Comparator('data/test.csv', 'res.csv').compare(databases)


if __name__ == '__main__':
    compare()
