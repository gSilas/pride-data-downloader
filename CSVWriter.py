import csv
from MGFFile import MGFFile
from mzIdentMLFile import mzIdentMLFile


class DatasetCSVWriter(object):

    def __init__(self, path, header, mzIdent_list, mgf_list):
        self.path = path
        self.mzIdent_list = mzIdent_list
        self.mgf_list = mgf_list
        self.header = header
        print(header)

    def write_data_csv(self):
        with open(self.path, 'w', newline='') as csvfile:
            csvwriter = csv.DictWriter(csvfile, delimiter=',', fieldnames=self.header)
            csvwriter.writeheader()

            for mgf, mzid in zip(self.mgf_list, self.mzIdent_list):
                for key in mgf.entries:
                    if key in mzid.entries:
                        print(key)
                        csvwriter.writerow({**mgf.entries[key], **mzid.entries[key]})

        
if __name__ == "__main__":
    mgf_list = []
    mzid_list = []

    for i in range(1, 52):
        mgf_zero = MGFFile()
        mgf_zero.parse_mgf('data_pride/' + str(i) + '.mgf')
        mgf_list.append(mgf_zero)
        mzident_zero = mzIdentMLFile()
        mzident_zero.parse_mzident('data_pride/' + str(i) + '.mzid')
        mzid_list.append(mzident_zero)
        print(i)

    entrycsv = DatasetCSVWriter('test.csv', header=mgf_zero.dict_tokens+mzident_zero.tokens, mgf_list=mgf_list, mzIdent_list=mzid_list)
    entrycsv.write_data_csv()