import jsonpickle
import csv
from mgf_file import parse_mgf
from mzidentml_file import parse_mzident


def serializePSM(psm):
    return jsonpickle.encode(psm, unpicklable=False)


def writeJSONPSM(filename, psm):
    with open(filename, 'w') as fp:
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
        mzid = parse_mzident(mzidfp)
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