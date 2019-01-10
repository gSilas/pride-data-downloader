import re
import xml.etree.cElementTree as ET


class mzIdentMLFile(object):

    def __init__(self):
        self.entries = dict()
        self.tokens = ['sequence', 'decoy']

    def parse_mzident(self, mzid_file):
        # Parsing mzid-file as XML file
        # accessing peptide sequence and spectrum id
        try:
            mzid_xmltree = ET.parse(mzid_file)
        except (ET.ParseError, ValueError) as err:
            print("File: " + str(mzid_file) + " is bad!")
            print(err)
            print(err.args)
            return self.entries

        for analysisSoftwareList in mzid_xmltree.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}AnalysisSoftwareList'):
            self.entries['software'] = list()
            for software in analysisSoftwareList.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}AnalysisSoftware'):
                if 'name' in software:
                    self.entries['software'].append(software.attrib['name'])
                else:
                    self.entries['software'].append(software.attrib['id'])

        for spectrumIdentificationResult in mzid_xmltree.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}SpectrumIdentificationResult'):
            # Sanity check for valid SpectrumID
            if re.search(r'index=\d+$', spectrumIdentificationResult.attrib['spectrumID']):

                for spectrumIdentificationItem in spectrumIdentificationResult.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}SpectrumIdentificationItem'):

                    entry = {'sequence': None, 'modification': {
                        'delta': None, 'location': None}, 'rank': None, 'decoy': None, 'params': dict()}
                    peptideRef = spectrumIdentificationItem.attrib['peptide_ref']
                    entry['rank'] = spectrumIdentificationItem.attrib['rank']

                    for peptide in mzid_xmltree.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}Peptide'):
                        if peptide.attrib['id'] == peptideRef:
                            for peptideSequence in peptide.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}PeptideSequence'):
                                entry['sequence'] = peptideSequence.text
                            for mod in peptide.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}Modification'):
                                entry['modification']['delta'] = mod.attrib['monoisotopicMassDelta']
                                entry['modification']['location'] = mod.attrib['location']

                    for evidence in mzid_xmltree.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}PeptideEvidence'):
                        if evidence.attrib['peptide_ref'] == peptideRef:
                            entry['decoy'] = 0 if evidence.attrib['isDecoy'] == 'false' else 1

                    for cvParam in spectrumIdentificationItem.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}cvParam'):
                        entry['params'][cvParam.attrib['name']
                                        ] = cvParam.attrib['value']

                    self.entries[spectrumIdentificationResult.attrib['spectrumID']] = entry

        return self.entries


if __name__ == '__main__':
    mgf_zero = mzIdentMLFile()
    mgf_zero.parse_mzident('data_pride/10.mzid')
    for key in mgf_zero.entries:
        print(key, repr(mgf_zero.entries[key]))
