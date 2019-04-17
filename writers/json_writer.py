import jsonpickle
import csv
from parsers.mgf_file import parse_mgf
from parsers import mzid_handler


def serializePSM(psm):
    """ 
    Serializes a PSM to json 
    
    Parameters
    ----------
    psm: dict
        dictionary representing a psm
    
    Returns
    -------
    dict
        dictionary representing a json
    """
    return jsonpickle.encode(psm, unpicklable=False)


def writeJSONPSM(filename, psm):
    """ 
    Writes a PSM json 
    
    Parameters
    ----------
    filename: str
        filename of the json
    psm: dict
        dictionary representing a psm
    """
    with open(filename, 'w') as fp:
        fp.write(jsonpickle.encode(psm, unpicklable=False))


def writeJSONPSMSfromArchive(archivePath, jsonPath):
    """
    Generates and writes PSM Jsons 
    
    Parameters
    ----------
    archivePath: str
        path to archive file
    jsonPath: str
        path to json

    """
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
        mzid = mzid_handler.MZIdentMLHandler().parse(mzidfp)
        mgf, _ = parse_mgf(mgffp)
        for key in mzid:
            if key in mgf:
                if int(mgf[key]['pepmass']) == int(float(mzid[key]['pepmass'])):
                    psms.append({'index': key, 'mgf': mgf[key],'mzid': mzid[key]})
                else:
                    print("No matching pepmass: {}".format(key))
            else:
                print("Not found in mgf: {}".format(key))

    writeJSONPSM(jsonPath, psms)

if __name__ == "__main__":
    archivePath = "data_pride/archive"
    jsonPath = "test.json"
    writeJSONPSMSfromArchive(archivePath, jsonPath)