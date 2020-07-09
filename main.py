from data import Data
from databases.db_ip import DBIP
from databases.geo_lite import GeoLite
from databases.ip2location import IP2Location


def main():
    databases = (
        IP2Location('data/IP2LOCATION-LITE-DB5.CSV'),
        DBIP('data/dbip-city-lite-2020-07.csv'),
        GeoLite('data/GeoLite2-City-Locations-ru.csv')
    )

    Data('data/test.csv', 'res.csv', databases).start_compare()


if __name__ == '__main__':
    main()
