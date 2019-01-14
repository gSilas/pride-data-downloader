import jsonpickle
import model.PSM
import model.Peptide
from model import Spectrum, PSM, Peptide
import csv
import logging
from MGFFile import MGFFile
from mzIdentMLFile import mzIdentMLFile
from mzIdentMLStat import parse_mzident

def generatePSM(mzid, mgf):

    psms = []
    for key in mzid.entries:
        entries_mzid = mzid.entries[key]
        if key in mgf.entries:
            entries_mgf = mgf.entries[key]
            pep = Peptide.Peptide(entries_mzid['sequence'])
            spec = Spectrum.Spectrum(entries_mgf['title'], entries_mgf['charge'],
                                entries_mgf['pepmass'], None, entries_mgf['mz_list'], entries_mgf['intensity_list'])
            psms.append(PSM.PSM(pep, spec, None))

    return psms


def serializePSM(psm):
    return jsonpickle.encode(psm, unpicklable=False)


def writeJSONPSM(filename, psm):
    with open(filename, 'w') as fp:
        print(jsonpickle.encode(psm, unpicklable=False))
        fp.write(jsonpickle.encode(psm, unpicklable=False))

def writeJSONPSMSfromArchive(archivePath, jsonPath):
    archived_files = []
    with open(archivePath, 'r') as fp:
        csvreader = csv.reader(fp, delimiter=';')
        for row in csvreader:
            archived_files.append(row)
    
    print("Archived Files:")
    for files in archived_files:
       print(files)

    psms = []
    for files in archived_files:
        mgffp = files[1]
        mzidfp = files[2]
        mzid = mzIdentMLFile()
        mzid.parse_mzident(mzidfp)
        mgf = MGFFile()
        mgf.parse_mgf(mgffp)
        psms.append(generatePSM(mzid, mgf))
        
    writeJSONPSM(jsonPath, psms)
