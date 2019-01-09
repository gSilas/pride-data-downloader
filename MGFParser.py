import os
import re

class MGFParser(object):

    def __init__(self):
        self.entries = dict()
        self.tokens = dict()
        self.dict_tokens = ['title', 'pepmass', 'charge', 'mz_list', 'intensity_list']
        self.mgf_list = []
        self.entries_count = 0

    def generate_mgf_list(self, input_file):
        with open(input_file, 'r') as mgf_file:
            self.mgf_list = mgf_file.read().split('\n')
        return self.mgf_list

    def get_mgf_tokens(self):
        index = 0
        line = self.mgf_list[index]
        while not 'END IONS' in line:
            if '=' in line:
                self.tokens[line.split('=')[0]] = index
            if re.match(r'\d+\.\d+\t\d+\.\d+', line):
                self.tokens['LISTS'] = index
                break
            index += 1
            line = self.mgf_list[index]
        return self.tokens

    def parse_mgf(self, input_file):
        self.generate_mgf_list(input_file)
        self.get_mgf_tokens()

        index = 0
        line = self.mgf_list[index]

        while index < len(self.mgf_list):
            line = self.mgf_list[index]

            if 'BEGIN IONS' in line:
                attributes = dict()

                line = self.mgf_list[index + self.tokens['TITLE']]
                attributes['title'] = line[6:]

                line = self.mgf_list[index + self.tokens['PEPMASS']]
                attributes['pepmass'] = float(line.split('=')[1])

                line = self.mgf_list[index + self.tokens['CHARGE']]
                attributes['charge'] = int(line[7:-1])

                spectrum_index = index + self.tokens['LISTS']
                line = self.mgf_list[spectrum_index]
                mz_lst = []; intensity_lst = []

                while not 'END IONS' in line:
                    line_split = line.split('\t')
                    mz_lst.append(float(line_split[0]))
                    intensity_lst.append(float(line_split[1]))
                    spectrum_index += 1
                    line = self.mgf_list[spectrum_index]

                attributes['mz_list'] = mz_lst
                attributes['intensity_list'] = intensity_lst

                self.entries[attributes['title'].split(';')[2]] = attributes

            index += 1

        self.entries_count = spectrum_index
        return self.entries


if __name__ == '__main__':
    mgf_zero = MGFParser()
    mgf_zero.parse_mgf('data_pride/0.mgf')
    for key in mgf_zero.entries:
    	print(repr(mgf_zero.entries[key]))
    print(mgf_zero.tokens)
