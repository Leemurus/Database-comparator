from geoip2.errors import AddressNotFoundError
from geoip2.database import Reader

from databases.database import Database


class GeoLite(Database):
    """ru, en, de languages, country, continent, city"""

    def __init__(self, path):
        super().__init__(path)
        self._database = Reader(path)

    def _get_main_information(self, location):
        if location:
            return location.city.names.get('en', '')
        else:
            return ''

    def get_data(self, ips, proc_num, return_dict):
        result = []

        for ip in ips:
            try:
                location = self._database.city(Database.int2ip(ip))
            except AddressNotFoundError:
                location = None

            result.append(self._get_main_information(location))

        return_dict[proc_num] = result
