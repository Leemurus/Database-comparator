from operator import itemgetter


class Database(object):
    def __init__(self, path):
        self._file_csv = open(path, 'r')

    def _ip_in_range(self, ip, line):
        data = list(map(int, line.split(',')[:2]))
        return data[0] <= ip <= data[1]

    @staticmethod
    def _get_main_information(line):
        data = line.split(',')
        return data[3] + '; ' + data[4]

    def get_data(self, ips, proc_num, return_dict):
        self._file_csv.seek(0, 0)
        enumerate_ips = list([i, el, None] for i, el in enumerate(ips))
        enumerate_ips.sort(key=itemgetter(1))

        ips_pointer = 0
        for line in self._file_csv:
            if ips_pointer >= len(ips):
                break

            while ips_pointer < len(ips) and self._ip_in_range(enumerate_ips[ips_pointer][1], line):
                enumerate_ips[ips_pointer][2] = self._get_main_information(line)
                ips_pointer += 1

        return_dict[proc_num] = tuple(x[2] for x in sorted(enumerate_ips))

    @staticmethod
    def ip2int(ip):
        lst = map(int, ip.split('.'))
        res = lst[3] | lst[2] << 8 | lst[1] << 16 | lst[0] << 24
        return res

    @staticmethod
    def int2ip(int_ip):
        return '.'.join(list(str((int_ip >> i * 8) % 256) for i in reversed(range(4))))
