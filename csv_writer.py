import csv
from mgf_file import parse_mgf
from mzidentml_file import parse_mzident


class DatasetCSVWriter():

    def __init__(self, path, header, mzident_list, mgf_list):
        self.path = path
        self.mzident_list = mzident_list
        self.mgf_list = mgf_list
        self.header = header
        print(header)

    def write_data_csv(self):
        with open(self.path, 'w', newline='') as csvfile:
            csvwriter = csv.DictWriter(
                csvfile, delimiter=';', fieldnames=self.header)
            csvwriter.writeheader()

            for mgf, mzid in zip(self.mgf_list, self.mzident_list):
                for key in mgf:
                    if key in mzid:
                        print(key)
                        csvwriter.writerow({**mgf[key], **mzid[key]})


if __name__ == "__main__":
    mgf_list = list()
    mzid_list = list()

    for i in range(1, 52):
        mgf_zero, mgf_tokens = parse_mgf('data_pride/' + str(i) + '.mgf')
        mgf_list.append(mgf_zero)
        mzident_zero = parse_mzident('data_pride/' + str(i) + '.mzid')
        mzid_list.append(mzident_zero)
        print(i)

    entrycsv = DatasetCSVWriter('test.csv', header=mgf_tokens +
                                mzident_zero.keys(), mgf_list=mgf_list, mzident_list=mzid_list)
    entrycsv.write_data_csv()
