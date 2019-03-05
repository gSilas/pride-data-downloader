import xml.sax


class _Result(object):
    rank = ''
    modifications = []
    sequence = ''
    experimentalMassToCharge = ''
    calculatedMassToCharge = ''
    isDecoy = ''


def make_result(result_spec_ident, result_pep_evid, result_seq, result_mod):
    
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
        if result_mod:
            _internal_result.modifications = []
            for mod in result_mod:
                _internal_result.modifications.append((float(mod[0]), int(mod[1])))

    return _internal_result


class MZIdentMLHandler(xml.sax.handler.ContentHandler):
    def __init__(self):
        self._result_params = dict()

        self._result_spec_ident = dict()
        self._result_pep = list()
        self._result_pep_evid = dict()
        self._result_pep_seq = dict()
        self._result_mod = dict()

        self._peptide_ref = 0
        self._sequence = 1

        self._open_tags = list()

        self._charBuffer = []

        self._current_pep = str()
        self._current_spec_ident = str()
        self._current_spec_id = str()

    def parse(self, f):
        xml.sax.parse(f, self)

        results = dict()

        for peptide in self._result_pep:
            result =  make_result(
                self._result_spec_ident[peptide] if peptide in self._result_spec_ident else None, 
                self._result_pep_evid[peptide] if peptide in self._result_pep_evid else None, 
                self._result_pep_seq[peptide] if peptide in self._result_pep_seq else None, 
                self._result_mod[peptide] if peptide in self._result_mod else None)

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
            self._result_params['AnalysisSoftware'] = attrs.getValue('name')

        elif name == 'cvParam':
            if 'Modification' in self._open_tags:
                if self._current_pep in self._result_mod:
                    self._result_mod[self._current_pep][-1] + \
                        (attrs.getValue('name'),)

            elif 'FragmentTolerance' in self._open_tags:
                if "search tolerance plus value" in attrs.getValue('name'):
                    self._result_params['search tolerance plus value'] = float(
                        attrs.getValue('value'))
                elif "search tolerance minus value" in attrs.getValue('name'):
                    self._result_params['search tolerance minus value'] = float(
                        attrs.getValue('value'))

            elif "SpectrumIdentificationItem" in self._open_tags:
                if 'value' in attrs.getNames():
                    self._result_spec_ident[self._current_spec_ident] + \
                        (attrs.getValue('name'), attrs.getValue('value'))

        elif name == 'Modification':
            if self._current_pep in self._result_mod:
                self._result_mod[self._current_pep].append(
                    (attrs.getValue('monoisotopicMassDelta'), attrs.getValue('location')))
            else:
                self._result_mod[self._current_pep] = list()
                self._result_mod[self._current_pep].append(
                    (attrs.getValue('monoisotopicMassDelta'), attrs.getValue('location')))

        elif name == 'SpectrumIdentificationItem':
            self._current_spec_ident = attrs.getValue('peptide_ref')
            self._result_spec_ident[attrs.getValue('peptide_ref')] = (self._current_spec_id, attrs.getValue(
                'rank'), attrs.getValue('experimentalMassToCharge'),  attrs.getValue('calculatedMassToCharge'))

        elif name == 'SpectrumIdentificationResult':
            self._current_spec_id = attrs.getValue('spectrumID')

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
    with open("/home/dan/Documents/Work/PRIDEdata/xml_handlers/kelstrup_hela-res15000_all-fractions.out.cpsx.xml") as f:
        results = MZIdentMLHandler().parse(f)[0]

        for pep in results:
            print(results[pep].__dict__)
