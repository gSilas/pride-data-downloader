import re
import xml.etree.cElementTree as ET
import xml


def parse_mzident(mzid_file):
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
        #for pep_child in specid_child.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}cvParam'):
            #cvparams.append(pep_child.attrib['name'])
        break
    return software, cvparams


if __name__ == '__main__':

    out = []
    count = []

    for i in range(0, 58):
        print(i)
        try:
            tup = parse_mzident('data_pride/' + str(i) +  '.mzid')
            if tup not in out:
                out.append(tup)
                count.append(1)
            else:
                count[out.index(tup)] += 1
        except (xml.etree.ElementTree.ParseError, ValueError) as err:
            print("File: " + str(i) + " is bad!")
            print(err)
            print(err.args)

    print(out)
    print(count)