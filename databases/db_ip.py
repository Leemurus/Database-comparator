from databases.database import Database


class DBIP(Database):
    """english, country iso, city, long, lant"""

    def _ip_in_range(self, ip, line):
        data = list(map(lambda x: Database.ip2int(x), line.split(',')[:2]))
        return data[0] <= ip <= data[1]

    def _get_main_information(self, line):
        data = line.replace('\"', '').split(',')
        return data[5]
