from databases.database import Database


class IP2Location(Database):
    def _ip_in_range(self, ip, line):
        data = list(map(int, line.replace('\"', '').split(',')[:2]))
        return data[0] <= ip <= data[1]

    def _get_main_information(self, line):
        data = line.replace('\"', '').split(',')
        return data[3] + '; ' + data[4]
