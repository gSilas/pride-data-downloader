import xml.etree.cElementTree as ET
import xml
import csv

def write_csv(path, dictionary):
    with open(path, 'a+', newline='') as csvfile:
        csvwriter = csv.DictWriter(
            csvfile, delimiter=';', fieldnames=list(dictionary))
        csvwriter.writeheader()
        csvwriter.writerow(dictionary)

def parse_stat_mzident(mzid_file):
    software = []
    cvparams = []

    tree = ET.parse(mzid_file)
    for specid_child in tree.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}AnalysisSoftwareList'):
        for gchild in specid_child.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}AnalysisSoftware'):
            if 'name' in gchild:
                software.append(gchild.attrib['name'])
            else:
                software.append(gchild.attrib['id'])

    for specid_child in tree.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}SpectrumIdentificationResult'):
        for gchild in specid_child.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}SpectrumIdentificationItem'):
            for pep_child in gchild.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}cvParam'):
                cvparams.append(pep_child.attrib['name'])
        break
    return software, cvparams


if __name__ == '__main__':

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
            tup = parse_stat_mzident(files[2])
        except (xml.etree.ElementTree.ParseError, ValueError) as err:
            print("File is bad!")
            print(files)
            print(err)
            print(err.args)
            continue

        if str(tup[1]) in params_stat:
            params_stat[str(tup[1])] += 1
        else:
            params_stat[str(tup[1])] = 1

        if str(tup[0]) in software_stat:
            software_stat[str(tup[0])] += 1
        else:
            software_stat[str(tup[0])] = 1

        if it % 25 == 0:
            print(it)
            print(len(params_stat), len(software_stat))
            write_csv('stats/' + str(it) + '_params_stat.csv', params_stat)
            params_stat = dict()

            write_csv('stats/' + str(it) + '_software_stat.csv', software_stat)
            software_stat = dict()

            print(it/len(archived_files))
         
        it += 1
    
    write_csv('stats/' + str(it) + '_params_stat.csv', params_stat)
    params_stat = dict()

    write_csv('stats/' + str(it) + '_software_stat.csv', software_stat)
    software_stat = dict()
