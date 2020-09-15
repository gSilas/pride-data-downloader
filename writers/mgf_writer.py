import sys
import logging
import csv
import multiprocessing
from functools import partial
from multiprocessing import Pool
from parsers.mgf_file import parse_mgf
from parsers import mzid_handler
import math
import os
import time
import datetime
from xml.sax._exceptions import SAXParseException;

log = logging.getLogger('PrideData')

def writeMGFSfromArchive(folder, archivePath, maximalNumberofCores):
    """ Writes PSMs to MGFS from Archive """
    archived_files = dict()
    with open(archivePath, 'r') as fp:
        csvreader = csv.reader(fp, delimiter=';')
        for row in csvreader:
            if row[0] in archived_files:
                archived_files[row[0]].append(row[1:])
            else:
                archived_files[row[0]] = list()
                archived_files[row[0]].append(row[1:])

    csv_files = []
    log.info("Archived Files:")

    for project_id in archived_files:
        log.info(project_id)
        with open(os.path.join(folder, project_id + ".mgf"), 'a+') as res_mgf_file:
            for files in archived_files[project_id]:
                processFunction(files=files, path=os.path.dirname(archivePath), project_id=project_id, result_file=res_mgf_file)

def processFunction(files, path=None, project_id=None, result_file = None):
    """ 
    Data-parallel function generating CSV 
    
    Parameters
    ----------
    files: list
        list of file tuples
        
    """
    
    mgf_path = os.path.join(path, str(project_id), str(project_id)+".mgf")

    mgffp: str = files[0]
    mzidfp: str = files[1]
    #log.info('Processing MZID {}'.format(mzidfp))
    try: 
        mzid = mzid_handler.MZIdentMLHandler().parse(mzidfp)
    except (SAXParseException):
        print("MZID cant be parsed!")
        return

    #log.info('Processing MGF {}'.format(mgffp))
    with open(mgffp, 'r') as mgf_file:

        line: str = mgf_file.readline()
        
        while(line):
            assert ('BEGIN IONS' in line), "ERROR: mgf in wrong format"
            # header TITLE
            line = mgf_file.readline()

            spectrum_index: list = line.split(';')[2][:-1]

            try:

                sequence = mzid[spectrum_index]
                
                # TITLE
                title_line = mgf_file.readline()
                assert ('TITLE' in title_line), "ERROR: TITLE: mgf in wrong format: " + repr(title_line)

                # PEPMASS
                pepmass_line = mgf_file.readline()
                assert ('PEPMASS' in pepmass_line), "ERROR: PEPMASS: mgf in wrong format " + repr(pepmass_line)

                # CHARGE
                charge_line = mgf_file.readline()
                assert ('CHARGE' in charge_line), "ERROR: CHARGE: mgf in wrong format " + repr(charge_line)

                result_file.write("BEGIN IONS\n")
                result_file.write(title_line)
                result_file.write(pepmass_line)
                result_file.write(charge_line)

                # SEQ
                result_file.write("SEQ="+sequence+"\n")

                # other attributes and spectra
                while not 'END IONS' in line:
                    line = mgf_file.readline()
                    result_file.write(line)

                line = mgf_file.readline()

            except (KeyError, AssertionError):
                print(spectrum_index + " missing in mzid")
                print("mzid " + mzidfp)
                print("mgf " + mgffp)

                while not 'END IONS' in line:
                    line = mgf_file.readline()

                line = mgf_file.readline()





if __name__ == "__main__":
    writeMGFSfromArchive("data_pride/archive", 4)