import xml.sax

class MZIdentMLHandler(xml.sax.handler.ContentHandler):
    """Implements a SAX xml parser for mzids"""

    def __init__(self):
        self._peptide_ref_to_sequence = dict()
        self._peptide_ref_to_spectrum_id = dict()

        self._current_spectrum_id =""
        self._current_peptide_ref = ""

        self._open_tags = list()

        self._charBuffer = []

    def parse(self, f):
        """ 
        SAX parser for mzid extracting psm information. 
        
        Parameters
        ----------
        f: path
            MZID path
        
        Returns
        -------
        dict
            psm result
        
        """
        
        xml.sax.parse(f, self)

        result: dict = dict()

        for key in self._peptide_ref_to_spectrum_id:
            sequence: str = self._peptide_ref_to_sequence[key]
            result[self._peptide_ref_to_spectrum_id[key]] = sequence
        
        return result

    def _getCharacterData(self):
        data = ''.join(self._charBuffer).strip()
        self._charBuffer = []
        return data.strip()  # remove strip() if whitespace is important

    def startElement(self, name, attrs):
        self._open_tags.append(name)

        if name == 'SpectrumIdentificationItem':
            self._peptide_ref_to_spectrum_id[attrs.getValue('peptide_ref')] = self._current_spectrum_id

        elif name == 'SpectrumIdentificationResult':
            self._current_spectrum_id = attrs.getValue('spectrumID').split()[0]

        elif name == 'Peptide':
            self._current_peptide_ref = attrs.getValue('id')

    def endElement(self, name):
        if name == 'PeptideSequence':
            self._peptide_ref_to_sequence[self._current_peptide_ref] = self._getCharacterData()

        self._open_tags.remove(name)

    def characters(self, data):
        if 'PeptideSequence' in self._open_tags:
            self._charBuffer.append(data)


if __name__ == "__main__":
    with open("/home/dan/Documents/Work/PRIDEdata/data_pride/PXD012407/ML_170312_Qex_B03.mzid") as f:
        results, parameters = MZIdentMLHandler().parse(f)
        print(results)
        for pep in results:
            print(results[pep].__dict__)
