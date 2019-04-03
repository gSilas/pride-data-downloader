import sys
import logging
import csv
import multiprocessing
from multiprocessing import Pool
from mgf_file import parse_mgf
from xml_handlers import mzid_handler
from mzidentml_file import parse_mzident
from peptide_calc import dm_dalton_ppm
from peptide_calc import SeriesMatcher
from peptide_labeler import class_label
import math

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

HEADER = ["Id", "Domain_Id", "Charge", "sumI", "norm_high_peak_intensity", "Num_of_Modifications", "Pep_Len", "Num_Pl", 
"mh(group)", "mh(domain)", "uniqueDM", "uniqueDMppm", "Sum_match_intensities", "Log_sum_match_intensity", "b+_ratio", 
"b++_ratio", "y+_ratio", "y++_ratio", "b+_count", "b++_count", "y+_count", "y++_count", "b+_long_count", 
# "b+_intensities",  "b++_intensities",  "y+_intensities",  "y++_intensities",
"b++_long_count", "y+_long_count", "y++_long_count", "median_matched_frag_ion_errors", "mean_matched_frag_ion_errors", 
"iqr_matched_frag_ion_errors", "Class_Label", "ClassLabel_Decision", "Params"]

def writeCSVRows(rows, csvPath):
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
        csvwriter = csv.DictWriter(csvfile, delimiter=';', fieldnames=HEADER)
        csvwriter.writeheader()

        for row in rows:
            csvwriter.writerow(row)

def generateRow(mzid, mgf, parameters):
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
    sequence = mzid.sequence
    modifications = mzid.modifications
    zipped_spectrum = zip(mgf['mz_list'], mgf['intensity_list']) 
    
    mods = [0.0] * len(sequence)
    for i in modifications:
        mods[i[1]-1] = i[0]

    decision, label = class_label(mzid)
    
    match = SeriesMatcher(sequence, mods, zipped_spectrum, parameters['search tolerance plus value'], parameters['search tolerance minus value'])
    if match.calculate_matches():
        dm_dalton, dm_ppm = dm_dalton_ppm(mzid.calculatedMassToCharge, mgf['pepmass'])

        row = {"Id": "UNDEFINED", 
        "Domain_Id": "UNDEFINED",
        "Charge": mgf['charge'],
        "sumI": sum(mgf['intensity_list']), 
        "norm_high_peak_intensity": match.highest_intensity/match.intensity_sum, # int highest peak / sum of intensities
        "Num_of_Modifications": len(mzid.modifications),
        "Pep_Len": len(mzid.sequence),
        "Num_Pl": len(mzid.modifications)/len(mzid.sequence), # num of mods / peptide length
        "mh(group)": float(mgf['pepmass']), # m + h mass experimental
        "mh(domain)": mzid.calculatedMassToCharge, # m + h mass calculated
        "uniqueDM": dm_dalton,
        "uniqueDMppm": dm_ppm,
        "Sum_match_intensities": match.sum_matched_intensities,
        "Log_sum_match_intensity": match.log_sum_matched_intensities,
        "b+_ratio": match.bplus_ratio,
        "b++_ratio": match.bplusplus_ratio, 
        "y+_ratio": match.yplus_ratio,
        "y++_ratio": match.yplusplus_ratio, 
        "b+_count": match.bplus_matches, 
        "b++_count": match.bplusplus_matches, 
        "y+_count": match.yplus_matches,
        "y++_count": match.yplusplus_matches,
        "b+_long_count": match.bplus_series,
        "b++_long_count": match.bplusplus_series, 
        "y+_long_count": match.yplus_series,
        "y++_long_count": match.yplusplus_series,
        "median_matched_frag_ion_errors": match.median_matching_error,
        "mean_matched_frag_ion_errors": match.mean_matching_error,
        "iqr_matched_frag_ion_errors": match.iqr_matching_error, 
        "Class_Label": label,
        "ClassLabel_Decision": decision#,
        #"Params": mzid['meta_parameters']
        }
        return row
    else:
        return None

def writeCSVPSMSfromArchive(archivePath, maximalNumberofCores):
    """ Writes PSMs to CSV from Archive """
    archived_files = []
    with open(archivePath, 'r') as fp:
        csvreader = csv.reader(fp, delimiter=';')
        for row in csvreader:
            archived_files.append(row)
    log.info("Archived Files:")
    for files in archived_files:
        log.info(files)

    processes = min(multiprocessing.cpu_count(), maximalNumberofCores)
    with Pool(processes=processes) as p:
        res = p.map_async(processFunction, archived_files)
        log.info(res.get())

       # processFunction(files)
        
def processFunction(files):
    """ 
    Data-parallel function generating CSV 
    
    Parameters
    ----------
    files: list
        list of file tuples
        
    """
    rows = []
    mgffp = files[1]
    mzidfp = files[2]
    log.info('Processing MZID {}'.format(mzidfp))
    mzid, parameters = mzid_handler.MZIdentMLHandler().parse(mzidfp)

    if not 'search tolerance minus value' and 'search tolerance plus value' in parameters:
        log.error('No tolerances found: {}'.format(mzidfp))

    log.info('Processing MGF {}'.format(mgffp))
    mgf, _ = parse_mgf(mgffp)

    for key in mzid:

        if key not in mgf:
            log.error("Not found in mgf: {}".format(key))
            continue

        if not (int(mgf[key]['pepmass']) == int(float(mzid[key].experimentalMassToCharge))):
            log.error("No matching pepmass: {}".format(key))
            continue 

        mgf_dict = mgf[key]
        mzid_dict = mzid[key]
        row = generateRow(mzid_dict, mgf_dict, parameters)
        if row:
            rows.append(row)
        else:
            log.error("No matching peaks: {}".format(key))
            
    log.info('Writing CSV!')
    writeCSVRows(rows, mgffp+".csv")

if __name__ == "__main__":
    writeCSVPSMSfromArchive("data_pride/archive", 4)
