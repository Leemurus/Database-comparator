from databases.database import Database
from multiprocessing import Process, Manager


class Data:
    def __init__(self, path, res_file, databases):
        self._file_csv = open(path, 'r')
        self._res_file = open(res_file, 'w')
        self._databases = databases
        self._init_res_file()

    def _init_res_file(self):
        self._res_file.write('IP,test result')
        for database in self._databases:
            self._res_file.write(',' + type(database).__name__)
        self._res_file.write('\n')

    def start_compare(self):
        ips = list(Database.ip2int(line.split(',')[0]) for line in self._file_csv)

        processes = []
        return_dict = Manager().dict()
        for proc_num, database in enumerate(self._databases):
            process = Process(target=database.get_data, args=(ips, proc_num, return_dict))
            processes.append(process)
            process.start()

        for process in processes:
            process.join()

        results = []
        for proc_num in sorted(return_dict.keys()):
            results.append(return_dict[proc_num])

        self._file_csv.seek(0, 0)
        for index, line in enumerate(self._file_csv):
            self._res_file.write(Database.int2ip(ips[index]))  # ip
            self._res_file.write(',' + line.split(',')[1].rstrip())  # result from test

            for result in results:
                self._res_file.write(',' + result[index])

            self._res_file.write('\n')
            self._res_file.flush()
