import xml
import csv
import gc
import time
import sys
from xml_handlers import statistics_handler
from utils import get_memory
from utils import memory_limit

def write_csv(path, dictionary):
    """ 
    Writes Statistics from MZID to CSV 
    
    Parameters
    ----------
    path: str
        output path of csv
    dictionary: dict
        dictionary representing csv
    """
    with open(path, 'a+', newline='') as csvfile:
        csvwriter = csv.DictWriter(
            csvfile, delimiter=';', fieldnames=list(dictionary))
        csvwriter.writeheader()
        csvwriter.writerow(dictionary)
        print("{} written!".format(path))

def parse_stat_mzident(mzid_file):
    """ 
    Parses MZID and generates statistics 
    
    Parameters
    ----------
    mzid_file: str
        path to mzid file

    Returns
    -------
    dict
        statistics dictionary
    """
    print(mzid_file)
    with open(mzid_file) as f:
        return statistics_handler.StatisticsHandler().parse(f)

def main():
    memory_limit(0.8) # Limitates maximun memory usage to half

    archived_files = []
    with open('data_pride/archive', 'r') as fp:
       csvreader = csv.reader(fp, delimiter=';')
       for row in csvreader:
           archived_files.append(row)
    it = 1
    params_stat = dict()
    software_stat = dict()
    for files in archived_files:

        try:
            print("Start Parsing!")
            dictionary = parse_stat_mzident(files[2])
            print("Finished Parsing!")
        except (xml.etree.ElementTree.ParseError, ValueError, MemoryError) as err:
            print("File is bad!")
            print(files)
            print(err)
            print(err.args)
            gc.collect()
            time.sleep(5)
            continue

        if str(dictionary['params']) in params_stat:
            params_stat[str(dictionary['params'])] += 1
        else:
            params_stat[str(dictionary['params'])] = 1

        if str(dictionary['software']) in software_stat:
            software_stat[str(dictionary['software'])] += 1
        else:
            software_stat[str(dictionary['software'])] = 1

        print()
    
    write_csv('stats/' + str(it) + '_params_stat.csv', params_stat)
    write_csv('stats/' + str(it) + '_software_stat.csv', software_stat)

if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        print(err)
