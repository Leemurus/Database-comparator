from binascii import hexlify
from struct import unpack
from typing import Union

from databases.pysyge import GeoLocator
import socket
import struct
import csv


class DatToCsv:
    class Writer:
        def __init__(self, path, lang):
            self._file = csv.writer(open(path, 'w'))
            self._lang = lang

        def write(self, prev_ipn, ipn, location):
            prev_ip = socket.inet_ntoa(struct.pack("!I", prev_ipn))
            ip = socket.inet_ntoa(struct.pack("!I", ipn))
            # self._file.writerow((
            #     prev_ip + '-' + ip,
            #     location
            # ))
            self._file.writerow((
                prev_ip + '-' + ip,
                location['city'].get('name_' + self._lang),
                location['region'].get('name_' + self._lang),
                location['country'].get('name_' + self._lang),
                location['city'].get('lat'),
                location['city'].get('lon')
            ))

    def __init__(self, db_file):
        self._fh = open(db_file, 'rb')
        self._region_cache = {}

        header = self._fh.read(40)

        self._header = dict(zip(
            ('ver', 'ts', 'type', 'charset', 'b_idx_len',
             'm_idx_len', 'range', 'db_items', 'id_len', 'max_region',
             'max_city', 'region_size', 'city_size', 'max_country',
             'country_size', 'pack_size'),
            unpack('>BLBBBHHLBHHLLHLH', header[3:])))

        self._b_idx_len = self._header['b_idx_len']
        self._m_idx_len = self._header['m_idx_len']
        self._db_items = self._header['db_items']
        self._range = self._header['range']
        self._id_len = self._header['id_len']
        self._block_len = self._id_len + 3
        self._max_region = self._header['max_region']
        self._max_city = self._header['max_city']
        self._max_country = self._header['max_country']
        self._country_size = self._header['country_size']
        self._db_ver = self._header['ver']
        self._db_ts = self._header['ts']

        self._pack = self._fh.read(self._header['pack_size']).split(b'\0') if self._header['pack_size'] else ''

        self._b_idx_str = self._fh.read(self._header['b_idx_len'] * 4)
        self._m_idx_str = self._fh.read(self._header['m_idx_len'] * 4)
        self._db_begin = self._fh.tell()

        self._db = self._fh.read(self._db_items * self._block_len)
        self._db_regions = ''
        self._db_cities = ''

        self._db_regions = self._fh.read(self._header['region_size'])
        self._db_cities = self._fh.read(self._header['city_size'])

        self._info = {'regions_begin': self._db_begin + self._db_items * self._block_len}
        self._info['cities_begin'] = self._info['regions_begin'] + self._header['region_size']

    @staticmethod
    def chr_(val: Union[int, bytes]):
        try:
            return chr(val)

        except TypeError:
            pass

        return val

    _TYPE_COUNTRY = 0
    _TYPE_REGION = 1
    _TYPE_CITY = 2

    @staticmethod
    def _structure_location_data(city, country, region):
        if 'country_id' in city or 'region_seek' in city:
            del city['country_id']
            del city['region_seek']

        doc = {
            'city': city,
            'region': region,
            'country': country
        }

        return doc

    def _read_data_chunk(self, data_type: int, start_pos: int, max_read: int):
        raw = b''

        if start_pos and max_read:
            src = self._db_cities

            if data_type == self._TYPE_REGION:
                src = self._db_regions

            raw = src[start_pos:start_pos + max_read]

        return self._parse_pack(self._pack[data_type], raw)

    def _parse_pack(self, pack, item=b''):
        result = {}
        start_pos = 0
        empty = not item

        map_len = {
            't': 1, 'T': 1,
            's': 2, 'S': 2, 'n': 2,
            'm': 3, 'M': 3,
            'd': 8,
            'c': lambda: int(self.chr_(chunk_type[1:])),
            'b': lambda: item.find(b'\0', start_pos) - start_pos
        }
        map_val = {
            't': lambda: unpack('b', val),
            'T': lambda: unpack('B', val),
            's': lambda: unpack('h', val),
            'S': lambda: unpack('H', val),
            'm': lambda: unpack('i', val),  # TODO unpack('i', val + (ord(val[2]) >> 7 ? '\xff' : '\0'))
            'M': lambda: unpack('I', val + b'\0'),
            'i': lambda: unpack('i', val),
            'I': lambda: unpack('I', val),
            'f': lambda: unpack('f', val),
            'd': lambda: unpack('d', val),
            'n': lambda: unpack('h', val)[0] / pow(10, int(self.chr_(chunk_type[1]))),
            'N': lambda: unpack('i', val)[0] / pow(10, int(self.chr_(chunk_type[1]))),
            'c': lambda: val.rstrip(b' '),
        }

        for chunk in pack.split(b'/'):

            chunk_type, chunk_name = chunk.split(b':')
            chunk_name = chunk_name.decode()
            type_letter = self.chr_(chunk_type[0])

            if empty:
                result[chunk_name] = '' if type_letter in {'b', 'c'} else 0
                continue

            length = map_len.get(type_letter, 4)
            chars = type_letter in {'c', 'b'}

            if chars:
                length = length()

            end_pos = start_pos + length
            val: bytes = item[start_pos:end_pos]
            val_real = map_val.get(type_letter)

            if val_real is None:  # case `b`
                val_real = val
                length += 1

            else:
                val_real = val_real()

            start_pos += length

            if chars:
                val_real = val_real.decode()

            result[chunk_name] = val_real

            if isinstance(val_real, tuple):
                result[chunk_name] = val_real[0]

        return result

    def _get_end_of_ranges(self):
        ends = []

        for i in range(1, self._b_idx_len):
            start = (i - 1) * 4
            blocks = dict(zip(('min', 'max'), unpack('>LL', self._b_idx_str[start:start + 8])))
            ends.append(blocks['max'])

        return ends


    def _cached(func):
        def decorator(self, seek):
            if seek in self._region_cache:
                return self._region_cache[seek]

            result = func(self, seek)
            self._region_cache[seek] = result
            return result

        return decorator

    @_cached
    def get_location_by_seek(self, seek):
        country_only = False
        try:
            if seek < self._country_size:
                country = self._read_data_chunk(self._TYPE_COUNTRY, seek, self._max_country)
                city = self._parse_pack(self._pack[2])
                country_only = True
                city['lat'] = country['lat']
                city['lon'] = country['lon']

            else:
                city = self._read_data_chunk(self._TYPE_CITY, seek, self._max_city)
                country = {
                    'id': city['country_id'],
                }

            region = self._read_data_chunk(self._TYPE_REGION, city['region_seek'], self._max_region)
            if not country_only:
                country = self._read_data_chunk(self._TYPE_COUNTRY, region['country_seek'], self._max_country)
        except:
            city = {'name_en': Exception}
            country = {}
            region = {}

        return self._structure_location_data(city, country, region)

    def convert(self, csv_file, lang):
        writer = self.Writer(csv_file, lang)
        ends = self._get_end_of_ranges()
        first_part = 0
        prev_ip = 0
        prev_id = 0

        for i in range(0, self._db_items):
            start = i * self._block_len
            ip = int(hexlify(self._db[start: start + 3]), 16)
            id = int(hexlify(self._db[start + 3:start + 3 + self._id_len]), 16)

            full_prev_ip = (first_part << 24) + prev_ip
            if not first_part or i in ends:
                first_part += 1
            full_cur_ip = (first_part << 24) + ip - 1

            writer.write(full_prev_ip, full_cur_ip, self.get_location_by_seek(prev_id))
            # writer.write(full_prev_ip, full_cur_ip, prev_id)

            prev_ip = ip
            prev_id = id

        writer.write((first_part << 24) + prev_ip, 2 ** 32 - 1, self.get_location_by_seek(0))
        # writer.write((first_part << 24) + prev_ip, 2 ** 32 - 1, 0)


def main():
    db = DatToCsv('data/SxGeoCity.dat')
    db.convert('res3.csv', 'en')


def main3():
    csv_file = csv.writer(open('res12312.csv', 'w'))
    db = GeoLocator('data/SxGeoCity.dat', 1)
    left_border = '0.0.0.0'
    prev_id = 0
    MAX_IP = 2 ** 32 - 1

    for cur_ip in range(0, MAX_IP):
        ip = socket.inet_ntoa(struct.pack("!I", cur_ip))
        id = db.get_location(ip)

        if prev_id != id:
            csv_file.writerow((left_border + '-' + socket.inet_ntoa(struct.pack("!I", cur_ip - 1)), prev_id))
            prev_id = id
            left_border = ip

    csv_file.writerow((left_border + '-' + socket.inet_ntoa(struct.pack("!I", MAX_IP - 1)), prev_id))


def correct():
    file = csv.reader(open('res2.csv'))
    out = csv.writer(open('res2.csv', 'w'))

    prev_id = 0
    for line in file:
        out.writerow((line[0], prev_id))
        prev_id = line[1]


def comparator():
    file1 = csv.reader(open('res.csv'))
    file2 = csv.reader(open('res2.csv'))
    out = csv.writer(open('comp.csv', 'w'))

    iter1 = file1.__iter__()
    iter2 = file2.__iter__()
    while True:
        line1 = iter1.__next__()
        line2 = iter2.__next__()
        if line1[0] != line2[0]:
            out.writerow(line1 + ['', ''] + line2 + ['ERROR'])
        else:
            out.writerow(line1 + ['', ''] + line2)


def get_ranges():
    db = DatToCsv('data/SxGeoCity.dat')

    for i in range(1, db._b_idx_len):
        start = (i - 1) * 4
        blocks = dict(zip(('min', 'max'), unpack('>LL', db._b_idx_str[start:start + 8])))
        print(str(i) + ' ' + str(blocks))


def get_location():
    db = GeoLocator('data/SxGeoCity.dat', 1)
    # print(db.get_location('70.174.61.0'))
    # print(db.parse_location(db.get_location('70.174.61.0')))

    from pprint import pprint
    ip = '1.0.74.0'
    pprint(db.get_location(ip))
    pprint(db._parse_location(db.get_location(ip), detailed=True))


if __name__ == '__main__':
    get_location()
