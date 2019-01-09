import re
import xml.etree.cElementTree as ET


class mzIdentMLParser(object):

    def __init__(self):
        self.entries = dict()
        self.tokens = ['sequence', 'decoy']

    def parse_mzident(self, mzid_file):
        # Parsing mzid-file as XML file
        # accessing peptide sequence and spectrum id
        tree = ET.parse(mzid_file)
        for specid_child in tree.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}SpectrumIdentificationResult'):
            if re.search('index=\d+$', specid_child.attrib['spectrumID']):
                for gchild in specid_child.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}SpectrumIdentificationItem'):
                    pep_ref = gchild.attrib['peptide_ref']
                    for pep_child in tree.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}Peptide'):
                        if pep_child.attrib['id'] == pep_ref:
                            for pepseq_child in pep_child.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}PeptideSequence'):
                                self.entries.update(
                                    {specid_child.attrib['spectrumID']: {'sequence': pepseq_child.text, 'decoy': None}}) 
                    for evidence in tree.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}PeptideEvidence'):
                        if evidence.attrib['peptide_ref'] == pep_ref:
                            self.entries[specid_child.attrib['spectrumID']]['decoy'] = 0 if evidence.attrib['isDecoy'] == 'false' else 1

        return self.entries

if __name__ == '__main__':
    mgf_zero = mzIdentMLParser()
    mgf_zero.parse_mzident('data_pride/0.mzid')
    for key in mgf_zero.entries:
    	print(key, repr(mgf_zero.entries[key]))