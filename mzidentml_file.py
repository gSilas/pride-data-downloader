import re
import xml.etree.cElementTree as ET


def parse_mzident(mzid_file):
    entries = dict()
    try:
        mzid_xmltree = ET.parse(mzid_file)
    except (ET.ParseError, ValueError) as err:
        print("File: " + str(mzid_file) + " is bad!")
        print(err)
        print(err.args)
        return entries

    for analysisSoftwareList in mzid_xmltree.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}AnalysisSoftwareList'):
        entries['software'] = list()
        for software in analysisSoftwareList.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}AnalysisSoftware'):
            if 'name' in software:
                entries['software'].append(software.attrib['name'])
            else:
                entries['software'].append(software.attrib['id'])

    for spectrumIdentificationResult in mzid_xmltree.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}SpectrumIdentificationResult'):
        # Sanity check for valid SpectrumID
        if re.search(r'index=\d+$', spectrumIdentificationResult.attrib['spectrumID']):

            for spectrumIdentificationItem in spectrumIdentificationResult.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}SpectrumIdentificationItem'):

                entry = {'sequence': None, 'modification': {
                    'delta': None, 'location': None}, 'pepmass':None, 'rank': None, 'decoy': None, 'params': dict()}
                peptideRef = spectrumIdentificationItem.attrib['peptide_ref']
                entry['rank'] = spectrumIdentificationItem.attrib['rank']
                entry['pepmass'] = spectrumIdentificationItem.attrib['experimentalMassToCharge']

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
                    if 'value' in cvParam.attrib:
                        entry['params'][cvParam.attrib['name']
                                        ] = cvParam.attrib['value']

                entries[spectrumIdentificationResult.attrib['spectrumID']] = entry

    return entries


if __name__ == '__main__':
    mzid_zero = parse_mzident('data_pride/PXD007148/CAA5455_A549.mzid')
    for key in mzid_zero:
        print(key, repr(mzid_zero[key]))
