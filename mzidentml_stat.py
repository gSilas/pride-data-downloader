import defusedxml.ElementTree as ET
import xml
import csv
import gc
import resource
import time
import sys

def write_csv(path, dictionary):
    with open(path, 'a+', newline='') as csvfile:
        csvwriter = csv.DictWriter(
            csvfile, delimiter=';', fieldnames=list(dictionary))
        csvwriter.writeheader()
        csvwriter.writerow(dictionary)
        print("{} written!".format(path))

def memory_limit(ratio):
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    resource.setrlimit(resource.RLIMIT_AS, (int(get_memory() * 1024 * ratio), hard))

def get_memory():
    with open('/proc/meminfo', 'r') as mem:
        free_memory = 0
        for i in mem:
            sline = i.split()
            if str(sline[0]) in ('MemFree:', 'Buffers:', 'Cached:'):
                free_memory += int(sline[1])
    return free_memory

def parse_stat_mzident(mzid_file):
    print(mzid_file)
    software = []
    cvparams = []
    parsed = ET.parse(mzid_file)
    root = parsed.getroot()
    for specid_child in root.findall('{http://psidev.info/psi/pi/mzIdentML/1.1}AnalysisSoftwareList'):
        for gchild in specid_child.findall('{http://psidev.info/psi/pi/mzIdentML/1.1}AnalysisSoftware'):
            if 'name' in gchild:
                software.append(gchild.attrib['name'])
            else:
                software.append(gchild.attrib['id'])

    for data in root.findall('{http://psidev.info/psi/pi/mzIdentML/1.1}DataCollection'):
        for analysis_data in data.findall('{http://psidev.info/psi/pi/mzIdentML/1.1}AnalysisData'):
            for specid in analysis_data.findall('{http://psidev.info/psi/pi/mzIdentML/1.1}SpectrumIdentificationList'):
                for specid_child in specid.findall('{http://psidev.info/psi/pi/mzIdentML/1.1}SpectrumIdentificationResult'):
                    for gchild in specid_child.findall('{http://psidev.info/psi/pi/mzIdentML/1.1}SpectrumIdentificationItem'):
                        for pep_child in gchild.findall('{http://psidev.info/psi/pi/mzIdentML/1.1}cvParam'):
                            cvparams.append(pep_child.attrib['name'])
                    break

    del root

    return software, cvparams

def main():
    memory_limit() # Limitates maximun memory usage to half

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
            tup = parse_stat_mzident(files[2])
            print("Finished Parsing!")
        except (xml.etree.ElementTree.ParseError, ValueError, MemoryError) as err:
            print("File is bad!")
            print(files)
            print(err)
            print(err.args)
            gc.collect()
            time.sleep(5)
            continue

        if str(tup[1]) in params_stat:
            params_stat[str(tup[1])] += 1
        else:
            params_stat[str(tup[1])] = 1

        if str(tup[0]) in software_stat:
            software_stat[str(tup[0])] += 1
        else:
            software_stat[str(tup[0])] = 1

        print()
    
    write_csv('stats/' + str(it) + '_params_stat.csv', params_stat)
    write_csv('stats/' + str(it) + '_software_stat.csv', software_stat)

if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        print(err)
