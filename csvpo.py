import sys
import logging
import csv
import multiprocessing
from functools import partial
from multiprocessing import Pool
from parsers.mgf_file import parse_mgf
from parsers import mzid_handler
from features.psm_features import FeatureList
import math
import os

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

handler = logging.FileHandler('debug.log', mode='w')
handler.setFormatter(logging.Formatter(
    fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
handler.setLevel(logging.DEBUG)
log.addHandler(handler)

handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter(
    fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
handler.setLevel(logging.INFO)
log.addHandler(handler)

def writeCSVRows(rows, csvPath, features):
    """ 
    Writes multiple rows to CSV 
    
    Parameters
    ----------
    rows: list
        rows for csv
    csvPath: str
        path to csv
    
    """
    with open(csvPath, 'a+', newline='') as csvfile:
        csvwriter = csv.DictWriter(csvfile, delimiter=',', fieldnames=features)

        for row in rows:
            csvwriter.writerow(row)

def writeCSVHeader(csvPath, features):
    """ 
    Writes multiple rows to CSV 
    
    Parameters
    ----------
    rows: list
        rows for csv
    csvPath: str
        path to csv
    
    """
    with open(csvPath, 'a+', newline='') as csvfile:
        csvwriter = csv.DictWriter(csvfile, delimiter=',', fieldnames=features)
        csvwriter.writeheader()

def generateRow(mzid, mgf, parameters, feature_list):
    """ 
    Generates an individual row for csv from PSM 
    
    Parameters
    ----------
    mzid: _Result
        mzid representation
    mgf: dict
        mgf representation
    parameters: dict
        parameters for a psm

    Returns
    -------
    dict
        representing a csv PSM row

    """
    
    features = FeatureList(mzid, mgf, parameters['search tolerance plus value'], parameters['search tolerance minus value'])
    
    if features.calculate_features():

        row = dict()
        for feature in feature_list:
            row[feature] = features.dictionary[feature]

        return row
    else:
        return None

def writeCSVPSMSfromArchive(archivePath, maximalNumberofCores, features = []):
    """ Writes PSMs to CSV from Archive """
    archived_files = dict()
    with open(archivePath, 'r') as fp:
        csvreader = csv.reader(fp, delimiter=';')
        for row in csvreader:
            if row[:1][0] in archived_files:
                archived_files[row[:1][0]].append(row[1:])
            else:
                archived_files[row[:1][0]] = list()
                archived_files[row[:1][0]].append(row[1:])

    csv_files = []
    log.info("Archived Files:")
    for project_id in archived_files:
        log.info(project_id)

        processes = min(multiprocessing.cpu_count(), maximalNumberofCores)
        with Pool(processes=processes) as p:
            results = p.map(partial(processFunction, features=features), archived_files[project_id])

        csv_path = os.path.join(os.path.dirname(archivePath), str(project_id), str(project_id)+".csv")
        writeCSVHeader(csv_path, features)
        csv_files.append(csv_path)

        log.info('Writing CSV! {}'.format(csv_path))
        for res in results:
            writeCSVRows(res, csv_path, features)
    
    return csv_files
      
def processFunction(files, features):
    """ 
    Data-parallel function generating CSV 
    
    Parameters
    ----------
    files: list
        list of file tuples
        
    """
    rows = []
    mgffp = files[0]
    mzidfp = files[1]
    log.info('Processing MZID {}'.format(mzidfp))
    mzid, parameters = mzid_handler.MZIdentMLHandler().parse(mzidfp)

    if not ('search tolerance minus value' in parameters and 'search tolerance plus value' in parameters):
        log.error('No tolerances found! {0}'.format(mzidfp))
        return None
    
    else:
        log.info('Processing MGF {}'.format(mgffp))
        mgf, _ = parse_mgf(mgffp)

        not_found_in_mgf = 0
        not_matching_pepmass = 0
        not_matching_peaks = 0

        for key in mzid:

            if key not in mgf:
                not_found_in_mgf += 1
                continue

            if not (int(mgf[key]['pepmass']) == int(float(mzid[key].experimentalMassToCharge))):
                not_matching_pepmass += 1
                continue 

            mgf_dict = mgf[key]
            mzid_dict = mzid[key]
            row = generateRow(mzid_dict, mgf_dict, parameters, features)
            if row:
                rows.append(row)
            else:
                not_matching_peaks += 1

        if not_found_in_mgf+not_matching_peaks+not_matching_pepmass > 0:
            log.error("MZID: {0} Not found in MGF: {1} No matching peaks: {2} No matching pepmass: {3}".format(mzidfp, not_found_in_mgf, not_matching_peaks, not_matching_pepmass))
        if len(rows) > 0:
            return rows
        else:
            return None


if __name__ == "__main__":
    writeCSVPSMSfromArchive("data_pride/archive", 4)
