import numpy
import numba
import math

from numba import jit
from pyteomics import mass

from mgf_file import parse_mgf
from mzidentml_file import parse_mzident

# constant masses
mass_water = mass.calculate_mass(formula='H2O')
mass_hydrogen = mass.calculate_mass(formula='H')

# transforming sequences to masssequences
# sequence is converted to list containing each amino acids mass


def transform_sequence_to_masssequence(sequence, mods):
    mass_sequence = []
    index = 0
    for i in sequence:
        mass_sequence.append(mass.fast_mass(i) + mods[index])
        index += 1
    return mass_sequence

# b series implementation using numba


@jit(nopython=True)
def calculate_b_Series(sequence, charge):

    b_series = []
    b_series.append(
        (sequence[0] + charge * mass_hydrogen - mass_water) / charge)

    for i in range(1, len(sequence)):
        b_series.append((b_series[i-1] + (sequence[i] - mass_water) / charge))

    return b_series

# y series implementation using numba
@jit(nopython=True)
def calculate_y_Series(sequence, charge):

    y_series = [0.0] * len(sequence)
    y_series[0] = ((sequence[len(sequence)-1] +
                    charge * mass_hydrogen) / charge)

    index = 1
    for i in range(len(sequence)-2, -1, -1):
        y_series[index] = (
            (y_series[index-1] + (sequence[i] - mass_water) / charge))
        index += 1

    return y_series

@jit(nopython=True)
def dm_dalton_ppm(calculated_mass, experimental_mass):
    dm_dalton = calculated_mass - experimental_mass
    dm_ppm = dm_dalton/calculated_mass*1000000.0
    return dm_dalton, dm_ppm
    
# matches a y or b series to a sorted and zipped spectrum (m/z, intensities)
@jit(nopython=True)
def match_series_and_spectrum(series, zipped_data, threshold):
    matched_peaks = []
    matched_intensities = []
    longest_series = 0

    series_index = 0
    current_series = 0
    matched = False
    upper = series[series_index] + threshold
    lower = series[series_index] - threshold
    for i in range(0, len(zipped_data)):
        if zipped_data[i][0] <= upper and zipped_data[i][0] >= lower:
            matched_peaks.append(zipped_data[i][0])
            matched_intensities.append(zipped_data[i][1])
            matched = True

        if zipped_data[i][0] > upper and (series_index + 1) < len(series):
            if(matched):
                current_series += 1
                if current_series > longest_series:
                    longest_series = current_series
                matched = False
            else:
                current_series = 0
            
            series_index += 1
            upper = series[series_index] + threshold
            lower = series[series_index] - threshold
            i -= 1

        elif (series_index + 1) >= len(series):
            return matched_peaks, matched_intensities, longest_series

    return matched_peaks, matched_intensities, longest_series

class SeriesMatcher(object):
    def __init__(self, sequence, mods, zipped_spectrum, threshold):
        self.masssequence = transform_sequence_to_masssequence(sequence, mods)
        self.zipped_data = sorted(zipped_spectrum, key=lambda x: x[0])
        self.threshold = threshold
        self.match_all()
    
    def match_all(self):
        self.bplus_peaks, self.bplus_int, self.bplus_series = match_series_and_spectrum(
            calculate_b_Series(self.masssequence, 1), self.zipped_data, self.threshold)
        self.yplus_peaks, self.yplus_int, self.yplus_series = match_series_and_spectrum(
            calculate_y_Series(self.masssequence, 1), self.zipped_data, self.threshold)
        self.bplusplus_peaks, self.bplusplus_int, self.bplusplus_series = match_series_and_spectrum(
            calculate_b_Series(self.masssequence, 2), self.zipped_data, self.threshold)
        self.yplusplus_peaks, self.yplusplus_int, self.yplusplus_series = match_series_and_spectrum(
            calculate_y_Series(self.masssequence, 2), self.zipped_data, self.threshold)
        self.bplus_matches = len(self.bplus_peaks)
        self.bplusplus_matches = len(self.bplusplus_peaks)
        self.yplus_matches = len(self.yplus_peaks)
        self.yplusplus_matches = len(self.yplusplus_peaks)
        self.matches = len(self.bplus_peaks) + len(self.bplusplus_peaks) + len(self.yplus_peaks) + len(self.yplusplus_peaks)
        self.sum_intensities = sum(self.bplus_int) + sum(self.bplusplus_int) + sum(self.yplus_int) + sum(self.yplusplus_int)
        if self.sum_intensities > 0:
            self.log_sum_intensities = math.log10(self.sum_intensities)
        else:
            self.log_sum_intensities = 0
        if self.matches > 0:
            self.bplus_ratio = self.bplus_matches/self.matches
            self.bplusplus_ratio = self.bplusplus_matches/self.matches
            self.yplus_ratio = self.yplus_matches/self.matches
            self.yplusplus_ratio = self.yplusplus_matches/self.matches
        else:
            self.bplus_ratio = 0
            self.bplusplus_ratio = 0
            self.yplus_ratio = 0
            self.yplusplus_ratio = 0


if __name__ == "__main__":
    sequence = 'PEPTIDE'
    charge = 2
    mods = [0.0] * len(sequence)

    mass_sequence = transform_sequence_to_masssequence(sequence, mods)
    print(mass_sequence)
    print(calculate_b_Series(mass_sequence, charge))
    print(calculate_y_Series(mass_sequence, charge))

    mgf, tokens = parse_mgf(
        'data_pride/PXD007963/trcRBC-1 (F001639_trcRBC-1).mzid_trcRBC-1_(F001639_trcRBC-1).pride.mgf')
    mzid = parse_mzident(
        'data_pride/PXD007963/trcRBC-1 (F001639_trcRBC-1).mzid')

    for key in mzid:
        if key in mgf:
            if int(mgf[key]['pepmass']) == int(float(mzid[key]['pepmass'])):
                sequence = mzid[key]['sequence']
                modifications = mzid[key]['modifications']
                zipped_spectrum = zip(
                    mgf[key]['mz_list'], mgf[key]['intensity_list'])

                mods = [0.0] * len(sequence)
                index = 0
                for i in modifications['location']:
                    mods[int(i)] = float(modifications['delta'][index])
                    index += 1

                print(sequence, match_all(sequence, mods, zipped_spectrum))
            else:
                print("No matching pepmass: {}".format(key))
        else:
            print("Not found in mgf: {}".format(key))
