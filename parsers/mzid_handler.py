import xml.sax

class _Result(object):
    """Representation of a PSM"""
    def __init__(self):
        self.rank = ''
        self.modifications = []
        self.sequence = ''
        self.experimentalMassToCharge = ''
        self.calculatedMassToCharge = ''
        self.isDecoy = ''
        self.parameters = []


def make_result(result_spec_ident, result_pep_evid, result_seq, result_mod, result_params):
    '''
    Creates a Result object for a specific PSM.

    Parameters
    ----------
    result_spec_ident: tuple
        Spectrum identifications contain experimental and calculated masses and rank.
    result_pep_evid: bool
        Peptide evidences expresses if a peptide is decoy.
    result_seq: str
        result_seq: Identified PSM peptide sequence
    result_mod: list
        Contains modifications on the identfified peptide.
    result_params: list
        Parameters of the identification provided by Analysis software.

    Returns
    -------
    _Result
        Result for the PSM

    '''  

    _internal_result = None

    if 'X' in result_seq or 'B' in result_seq or 'Z' in result_seq or 'J' in result_seq:
        return _internal_result

    if result_spec_ident and result_pep_evid and result_seq:
        _internal_result = _Result()
        _internal_result.calculatedMassToCharge = float(result_spec_ident[3])
        _internal_result.experimentalMassToCharge = float(result_spec_ident[2])
        _internal_result.isDecoy = True if result_pep_evid == 'true' else False
        _internal_result.rank = int(result_spec_ident[1])
        _internal_result.sequence = result_seq
        for elem in result_params:
            _internal_result.parameters.append(elem)
        if result_mod:
            for mod in result_mod:
                _internal_result.modifications.append((float(mod[0]), int(mod[1])))
    return _internal_result


class MZIdentMLHandler(xml.sax.handler.ContentHandler):
    """Implements a SAX xml parser for mzids"""

    def __init__(self):
        self._result_params = dict()

        self._result_spec_ident = dict()
        self._result_pep = list()
        self._result_pep_evid = dict()
        self._result_pep_seq = dict()
        self._result_mod = dict()
        self._result_ident_params = dict()

        self._peptide_ref = 0
        self._sequence = 1

        self._open_tags = list()

        self._charBuffer = []

        self._current_pep = str()
        self._current_spec_ident = str()
        self._current_spec_id = str()

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

        results = dict()

        for peptide in self._result_pep:
            result =  make_result(
                self._result_spec_ident[peptide] if peptide in self._result_spec_ident else None, 
                self._result_pep_evid[peptide] if peptide in self._result_pep_evid else None, 
                self._result_pep_seq[peptide] if peptide in self._result_pep_seq else None, 
                self._result_mod[peptide] if peptide in self._result_mod else None,
                self._result_ident_params[peptide] if peptide in self._result_ident_params else None)

            if result:
                results[self._result_spec_ident[peptide][0]] = result
        
        return results, self._result_params

    def _getCharacterData(self):
        data = ''.join(self._charBuffer).strip()
        self._charBuffer = []
        return data.strip()  # remove strip() if whitespace is important

    def startElement(self, name, attrs):
        self._open_tags.append(name)

        if name == 'AnalysisSoftware':
            if 'name' in attrs.getNames():
                self._result_params['AnalysisSoftware'] = attrs.getValue('name')
            else:
                self._result_params['AnalysisSoftware'] = attrs.getValue('id')

        elif name == 'cvParam':
            if 'Modification' in self._open_tags:
                if self._current_pep in self._result_mod:
                    self._result_mod[self._current_pep][-1] + \
                        (attrs.getValue('name'),)

            elif 'FragmentTolerance' in self._open_tags:
                if "search tolerance plus value" in attrs.getValue("name") and "dalton" in attrs.getValue("unitName"):
                    self._result_params['search tolerance plus value'] = float(
                        attrs.getValue('value'))
                elif "search tolerance minus value" in attrs.getValue("name") and "dalton" in attrs.getValue("unitName"):
                    self._result_params['search tolerance minus value'] = float(
                        attrs.getValue('value'))

            elif "SpectrumIdentificationItem" in self._open_tags:
                if 'value' in attrs.getNames():
                    if self._current_spec_ident in self._result_ident_params:
                        #print(self._current_spec_ident, (attrs.getValue('name'), attrs.getValue('value')))
                        self._result_ident_params[self._current_spec_ident].append((attrs.getValue('name'), attrs.getValue('value')))
                    else:
                        self._result_ident_params[self._current_spec_ident] = list()
                        #print(self._current_spec_ident, (attrs.getValue('name'), attrs.getValue('value')))
                        self._result_ident_params[self._current_spec_ident].append((attrs.getValue('name'), attrs.getValue('value')))

        elif name == 'Modification':
            if self._current_pep in self._result_mod:
                self._result_mod[self._current_pep].append(
                    (attrs.getValue('monoisotopicMassDelta'), attrs.getValue('location')))
            else:
                self._result_mod[self._current_pep] = list()
                self._result_mod[self._current_pep].append(
                    (attrs.getValue('monoisotopicMassDelta'), attrs.getValue('location')))

        elif name == 'userParam':
            if 'fragment_ion_tolerance' in attrs.getValue('name') and "dalton" in attrs.getValue("unitName"):
                self._result_params['search tolerance plus value'] = float(
                        attrs.getValue('value'))
                self._result_params['search tolerance minus value'] = float(
                        attrs.getValue('value'))

        elif name == 'SpectrumIdentificationItem':
            self._current_spec_ident = attrs.getValue('peptide_ref')
            self._result_spec_ident[attrs.getValue('peptide_ref')] = (self._current_spec_id, attrs.getValue(
                'rank'), attrs.getValue('experimentalMassToCharge'),  attrs.getValue('calculatedMassToCharge'))

        elif name == 'SpectrumIdentificationResult':
            self._current_spec_id = attrs.getValue('spectrumID').split()[0]

        elif name == 'Peptide':
            self._current_pep = attrs.getValue('id')
            self._result_pep.append(self._current_pep)

        elif name == 'PeptideEvidence':
            if 'isDecoy' in attrs.getNames():
                self._result_pep_evid[attrs.getValue('peptide_ref')] = attrs.getValue('isDecoy')

    def endElement(self, name):
        if name == 'PeptideSequence':
            self._result_pep_seq[self._current_pep] = self._getCharacterData()

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
