import logging
import csv
from parsers import mzid_handler
import os
from xml.sax._exceptions import SAXParseException;

log = logging.getLogger('PrideData')

def writeMGFSfromArchive(folder, archivePath):
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

    log.info("Archived Files:")

    for project_id in archived_files:
        log.info(project_id)
        with open(os.path.join(folder, project_id + ".mgf"), 'w') as res_mgf_file:
            for files in archived_files[project_id]:
                processFunction(file=files, result_file=res_mgf_file)

def processFunction(file, result_file = None):
    """ 
    Data-parallel function generating CSV 
    
    Parameters
    ----------
    file: list
        list of file tuples
        
    """
    
    mgffp: str = file[0]
    mzidfp: str = file[1]
    log.info('Processing MZID {}'.format(mzidfp))
    try: 
        mzid = mzid_handler.MZIdentMLHandler().parse(mzidfp)
    except (SAXParseException):
        print("MZID cant be parsed!")
        return

    log.info('Processing MGF {}'.format(mgffp))
    with open(mgffp, 'r') as mgf_file:

        line: str = mgf_file.readline()
        
        while(line):
            try:
                assert ('BEGIN IONS' in line), "ERROR: mgf in wrong format"

                # TITLE
                title_line = mgf_file.readline()
                assert ('TITLE' in title_line), "ERROR: TITLE: mgf in wrong format: " + repr(title_line)

                spectrum_index: list = title_line.split(';')[2][:-1]
                sequence = mzid[spectrum_index]

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

            except (KeyError, AssertionError) as e:
                print(e)
                print("mzid " + mzidfp)
                print("mgf " + mgffp)

                while not 'END IONS' in line:
                    line = mgf_file.readline()

                    if line == "": 
                        print("Extremely fucked up file!")
                        return

                line = mgf_file.readline()





if __name__ == "__main__":
    writeMGFSfromArchive("", archivePath="../data_pride/archive")