from databases.database import Database
from asyncio import get_event_loop, create_task


class IP2RusCity(Database):
    URL = ''

    def _ip_in_range(self, ip, line):
        data = list(map(int, line.replace('\"', '').split(',')[:2]))
        return data[0] <= ip <= data[1]

    def _get_main_information(self, line):
        data = line.replace('\"', '').split(',')
        return data[3] + '; ' + data[4]

    def _get_content(self, ip):
        pass

    def get_data(self, ips, proc_num, return_dict):
        result = []

        loop = get_event_loop()
        tasks = [create_task(self._get_content(ip)) for ip in ips]
