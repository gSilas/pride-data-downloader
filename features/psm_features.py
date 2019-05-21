import numpy
import numba
import math
import logging
import sys

from numba import jit
from pyteomics import mass

from features.psm_labeler import class_label

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

# constant masses
mass_water = mass.calculate_mass(formula='H2O')
mass_hydrogen = mass.calculate_mass(formula='H')

# transforming sequences to masssequences
# sequence is converted to list containing each amino acids mass


def transform_sequence_to_masssequence(sequence, mods):
    """
    Amino acids sequence to masssequence

    Parameters
    ----------
    sequence: str
        Sequence of a peptide
    mods: list
        Modifications of the peptide

    Returns
    -------
    list
        masses of indices
    """

    mass_sequence = []
    index = 0
    for i in sequence:
        mass_sequence.append(mass.fast_mass(i) + mods[index])
        index += 1
    return mass_sequence

# b series implementation using numba


@jit(nopython=True)
def calculate_b_Series(sequence, charge):
    """ 
    Calculate b series for given sequence relative to charge 
    
    Parameters
    ----------
    sequence: list
        Sequence of masses from peptide
    charge: int
        Charge of peptide

    Returns
    -------
    list
        b series
    """

    b_series = []
    b_series.append(
        (sequence[0] + charge * mass_hydrogen - mass_water) / charge)

    for i in range(1, len(sequence)):
        b_series.append((b_series[i-1] + (sequence[i] - mass_water) / charge))

    return b_series

# y series implementation using numba
@jit(nopython=True)
def calculate_y_Series(sequence, charge):
    """ 
    Calculate y series for given sequence relative to charge 
    
    Parameters
    ----------
    sequence: list
        Sequence of masses from peptide
    charge: int
        Charge of peptide

    Returns
    -------
    list
        b series
    """

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
    """ 
    Difference in dalton and ppm 
    
    Parameters
    ----------
    calculated_mass: int
        calculated mass of peptide sequence
        
    experimental_mass: int
        experimental mass of peptide sequence
   
    Returns
    -------
    tuple
        delta in ppm and delta in dalton
    """
    
    dm_dalton = calculated_mass - experimental_mass
    dm_ppm = (dm_dalton/calculated_mass)*1000000.0
    return dm_dalton, dm_ppm

@jit(nopython=True)
def mean(array):
    """ 
    Calculates mean of array
    
    Parameters
    ----------
    array: list
        list of integers

    Returns
    -------
    int
        mean
    """ 

    sum = 0.0
    for val in array:
        sum += val
    return sum/len(array)

@jit(nopython=True)
def median(array):
    """ 
    Calculates the median in an array. 

    Parameters
    ----------
    array: list
        list of integers

    Returns
    -------
    int
        median
    """

    if len(array) % 2 == 1:
        return array[int(len(array) / 2)]
    elif len(array) % 2 == 0:
        return (array[int(len(array) / 2) - 1] + array[int(len(array) / 2)])/2

@jit(nopython=True)
def match_series_and_spectrum(series, zipped_data, upperthreshold, lowerthreshold):
    """ 
    matches a y or b series to a sorted and zipped spectrum (m/z, intensities)
    
    Parameters
    ----------
    series: list
        list of floats representing a masses
    zipped_data: tuple
        tuple of data containing intensities and masses
    upperthreshold: float
        upper threshold of a match
    lowerthreshold: float
        lower threshold of a match

    Returns
    -------
    tuple
    """

    matched_peaks = []
    matched_intensities = []
    matching_errors = []

    longest_series = 0

    series_index = 0
    current_series = 0
    matched = False
    upper = series[series_index] + upperthreshold
    lower = series[series_index] - lowerthreshold
    for index in range(0, len(zipped_data)):

        if zipped_data[index][0] > upper and (series_index
         + 1) < len(series):
            if(matched):
                current_series += 1
                if current_series > longest_series:
                    longest_series = current_series
                matched = False
                error = 0
                matching_errors.append(error)
            else:
                current_series = 0
            series_index += 1
            upper = series[series_index] + upperthreshold
            lower = series[series_index] - lowerthreshold

        if zipped_data[index][0] <= upper and zipped_data[index][0] >= lower:
            matched_peaks.append(zipped_data[index][0])
            matched_intensities.append(zipped_data[index][1])
            matching_errors.append(abs(series[series_index] - zipped_data[index][0]))
            matched = True

    return matched_peaks, matched_intensities, longest_series, matching_errors

@jit(nopython=True)
def spectrum_statistics(zipped_data):
    """ 
    calculates highest intensity and sum of intensities 
    
    Parameters
    ----------
    zipped_data: tuple
        tuple of data containing intensities and masses

    Returns
    -------
    tuple
        highest intensities and sum of intensities
    """
    highest_intensity = 0.0
    intensity_sum = 0.0

    for i in range(0, len(zipped_data)):

        intensity_sum += zipped_data[i][1]

        if zipped_data[i][1] > highest_intensity:
            highest_intensity = zipped_data[i][1]

    return highest_intensity, intensity_sum

class FeatureList(object):
    """ 
    Represents features of psms 
    """

    def __init__(self, mzid, mgf, upperthreshold, lowerthreshold):
        self.mzid = mzid
        self.mgf = mgf
        self.upperthreshold = upperthreshold
        self.lowerthreshold = lowerthreshold
        self.dictionary = dict()
    
    def calculate_features(self):
        """ 
        Calculates features of psms 

        Returns
        -------
        bool
            success or failure
        """

        sequence = self.mzid.sequence
        modifications = self.mzid.modifications

        zipped_spectrum = zip(self.mgf['mz_list'], self.mgf['intensity_list']) 
    
        mods = [0.0] * len(sequence)
        for i in modifications:
            index = i[1]-1
            if not index >= len(mods):
                mods[index] = i[0]
            else:
                log.error("Modification location larger than sequence! len: {0} location: {1}".format(len(mods), index)) 
                return None

        decision, label = class_label(self.mzid)

        dm_dalton, dm_ppm = dm_dalton_ppm(self.mzid.calculatedMassToCharge, self.mgf['pepmass'])

        self.masssequence = transform_sequence_to_masssequence(sequence, mods)
        self.zipped_data = sorted(zipped_spectrum, key=lambda x: x[0])

        self.highest_intensity, self.intensity_sum = spectrum_statistics(self.zipped_data)
        #print(self.zipped_data)
        self.bplus_peaks, self.bplus_int, self.bplus_series, bplus_errors = match_series_and_spectrum(
            calculate_b_Series(self.masssequence, 1), self.zipped_data, self.upperthreshold, self.lowerthreshold)

        self.yplus_peaks, self.yplus_int, self.yplus_series, bplusplus_errors = match_series_and_spectrum(
            calculate_y_Series(self.masssequence, 1), self.zipped_data, self.upperthreshold, self.lowerthreshold)

        self.bplusplus_peaks, self.bplusplus_int, self.bplusplus_series, yplus_errors = match_series_and_spectrum(
            calculate_b_Series(self.masssequence, 2), self.zipped_data, self.upperthreshold, self.lowerthreshold)

        self.yplusplus_peaks, self.yplusplus_int, self.yplusplus_series, ypluplus_errors = match_series_and_spectrum(
            calculate_y_Series(self.masssequence, 2), self.zipped_data,
             self.upperthreshold, self.lowerthreshold)

        matching_errors = sorted(bplus_errors + bplusplus_errors + yplus_errors + ypluplus_errors)
        #print(matching_errors)
        if len(matching_errors) > 0:
            self.mean_matching_error = mean(matching_errors)
            self.median_matching_error = median(matching_errors)
            self.iqr_matching_error = numpy.subtract(*numpy.percentile(matching_errors, [75, 25]))

            self.bplus_matches = len(self.bplus_peaks)
            self.bplusplus_matches = len(self.bplusplus_peaks)
            self.yplus_matches = len(self.yplus_peaks)
            self.yplusplus_matches = len(self.yplusplus_peaks)
            self.matches = len(self.bplus_peaks) + len(self.bplusplus_peaks) + len(self.yplus_peaks) + len(self.yplusplus_peaks)
            self.sum_matched_intensities = sum(self.bplus_int) + sum(self.bplusplus_int) + sum(self.yplus_int) + sum(self.yplusplus_int)
            
            if self.sum_matched_intensities > 0:
                self.log_sum_matched_intensities = math.log10(self.sum_matched_intensities)
            else:
                self.log_sum_matched_intensities = 0
            
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
                

            self.dictionary = { "Id": "UNDEFINED", 
                                "Domain_Id": "UNDEFINED",
                                "Charge": self.mgf['charge'],
                                "sumI": sum(self.mgf['intensity_list']), 
                                "norm_high_peak_intensity": self.highest_intensity/self.intensity_sum, # int highest peak / sum of intensities
                                "Num_of_Modifications": len(self.mzid.modifications),
                                "Pep_Len": len(self.mzid.sequence),
                                "Num_Pl": len(self.mzid.modifications)/len(self.mzid.sequence), # num of mods / peptide length
                                "mh(group)": float(self.mgf['pepmass']), # m + h mass experimental
                                "mh(domain)": self.mzid.calculatedMassToCharge, # m + h mass calculated
                                "uniqueDM": dm_dalton,
                                "uniqueDMppm": dm_ppm,
                                "Sum_match_intensities": self.sum_matched_intensities,
                                "Log_sum_match_intensity": self.log_sum_matched_intensities,
                                "b+_ratio": self.bplus_ratio,
                                "b++_ratio": self.bplusplus_ratio, 
                                "y+_ratio": self.yplus_ratio,
                                "y++_ratio": self.yplusplus_ratio, 
                                "b+_count": self.bplus_matches, 
                                "b++_count": self.bplusplus_matches, 
                                "y+_count": self.yplus_matches,
                                "y++_count": self.yplusplus_matches,
                                "b+_long_count": self.bplus_series,
                                "b++_long_count": self.bplusplus_series, 
                                "y+_long_count": self.yplus_series,
                                "y++_long_count": self.yplusplus_series,
                                "median_matched_frag_ion_errors": self.median_matching_error,
                                "mean_matched_frag_ion_errors": self.mean_matching_error,
                                "iqr_matched_frag_ion_errors": self.iqr_matching_error, 
                                "Class_Label": label,
                                "ClassLabel_Decision": decision
                                }
            return True
        else:
            #log.warning("No matchings calculated!")
            return False


if __name__ == "__main__":
    pass
