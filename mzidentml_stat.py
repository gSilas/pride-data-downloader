import xml.etree.cElementTree as ET
import xml
import csv


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

    stat = dict()

    try:
        tup = parse_stat_mzident('data_pride/PXD007963/trcBiBP-2 (F001637_trcBiBP-2).mzid')
        if str(tup) not in stat:
            stat[str(tup)] = {'softwares': tup[0],
                                'params': tup[1], 'count': 1}
        else:
            stat[str(tup)]['count'] += 1
    except (xml.etree.ElementTree.ParseError, ValueError) as err:
        print("File is bad!")
        print(err)
        print(err.args)

    with open('stat.csv', 'w', newline='') as csvfile:
        csvwriter = csv.DictWriter(csvfile, delimiter=';', fieldnames=[
                                   'softwares', 'params', 'count'])
        csvwriter.writeheader()

        for key in stat:
            csvwriter.writerow(stat[key])
