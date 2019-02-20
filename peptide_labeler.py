def class_label(mzid):
        
        print(mzid['params'])
        if 'Mascot:identity threshold' in mzid['params'] and 'Mascot:score' in mzid['params']:
            if float(mzid['params']['Mascot:score']) > float(mzid['params']['Mascot:identity threshold']):
                return 'mascot', "TRUE"
            else:
                return 'mascot', "FALSE"

        if 'Scaffold:Peptide Probability' in mzid['params']:
            if float(mzid['params']['Scaffold:Peptide Probability']) >= 0.99:
                return 'scaffold', "TRUE"
            else:
                return 'scaffold', "FALSE"

        if int(mzid['rank']) == 1:
            return 'rank', "TRUE"
        elif int(mzid['rank']) > 1:
            return 'rank', "FALSE"
