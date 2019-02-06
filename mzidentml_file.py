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

    meta_parameters = dict()
    for analysisSoftwareList in mzid_xmltree.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}AnalysisSoftwareList'):
        meta_parameters['software'] = list()
        for software in analysisSoftwareList.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}AnalysisSoftware'):
            if 'name' in software:
                meta_parameters['software'].append(software.attrib['name'])
            else:
                meta_parameters['software'].append(software.attrib['id'])
    
    for analysisProtocolCollection in mzid_xmltree.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}AnalysisProtocolCollection'):
        meta_parameters['tolerances'] = [0, 0]
        for tolerances in analysisProtocolCollection.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}FragmentTolerance'):
            for cvParam in tolerances.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}cvParam'):
                if "search tolerance plus value" in cvParam.attrib['name']:
                    meta_parameters['tolerances'][0] = float(cvParam.attrib['value'])
                elif "search tolerance minus value" in cvParam.attrib['name']:
                    meta_parameters['tolerances'][1] = float(cvParam.attrib['value'])

    for spectrumIdentificationResult in mzid_xmltree.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}SpectrumIdentificationResult'):
        # Sanity check for valid SpectrumID
        if re.search(r'index=\d+$', spectrumIdentificationResult.attrib['spectrumID']):

            for spectrumIdentificationItem in spectrumIdentificationResult.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}SpectrumIdentificationItem'):

                entry = {'meta_parameters': meta_parameters, 'sequence': None, 'modifications': {
                    'delta': list(), 'location': list(), 'name': list()}, 'calcpepmass': None, 'pepmass': None, 'rank': None, 'decoy': None, 'params': dict()}

                peptideRef = spectrumIdentificationItem.attrib['peptide_ref']
                entry['rank'] = float(spectrumIdentificationItem.attrib['rank'])
                entry['pepmass'] = float(spectrumIdentificationItem.attrib['experimentalMassToCharge'])
                entry['calcpepmass'] = float(spectrumIdentificationItem.attrib['calculatedMassToCharge'])
 
                for peptide in mzid_xmltree.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}Peptide'):
                    if peptide.attrib['id'] == peptideRef:
                        for peptideSequence in peptide.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}PeptideSequence'):
                            entry['sequence'] = peptideSequence.text
                        for mod in peptide.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}Modification'):
                            entry['modifications']['delta'].append(float(mod.attrib['monoisotopicMassDelta']))
                            entry['modifications']['location'].append(float(mod.attrib['location']))
                            for cvParam in mod.iter('{http://psidev.info/psi/pi/mzIdentML/1.1}cvParam'):
                                if 'name' in cvParam.attrib:
                                    entry['modifications']['name'].append(cvParam.attrib['name'])

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
