def class_label(mzid):
        
        if 'Mascot:identity threshold' in mzid['params'] and 'Mascot:score' in mzid['params']:
            if float(mzid['params']['Mascot:score']) > float(mzid['params']['Mascot:identity threshold']):
                return 'mascot', 1
            else:
                return 'mascot', 0

        if 'Scaffold:Peptide Probability' in mzid['params']:
            if float(mzid['params']['Scaffold:Peptide Probability']) > 0.99:
                return 'scaffold', 1
            else:
                return 'scaffold', 0

        if int(mzid['rank']) == 1:
            return 'rank', 1
        elif int(mzid['rank']) > 1:
            return 'rank', 0
