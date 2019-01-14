from model.Spectrum import Spectrum
from model.Peptide import Peptide

class PSM(object):

    def __init__(self, peptide, spectrum, score):
        self.peptide = peptide
        self.spectrum = spectrum
        self.score = score