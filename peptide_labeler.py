def class_label(mzid):

   # print(mzid.parameters)
    for param in mzid.parameters:
        if 'Mascot:identity threshold' in param[0] and 'Mascot:score' in param[0]:
            if float(param[1]) > float(param[1]):
                return 'mascot', "TRUE"
            else:
                return 'mascot', "FALSE"

        if 'Scaffold:Peptide Probability' in param[0]:
            if float(param[1]) >= 0.99:
                return 'scaffold', "TRUE"
            else:
                return 'scaffold', "FALSE"

    if int(mzid.rank) == 1:
        return 'rank', "TRUE"
    elif int(mzid.rank) > 1:
        return 'rank', "FALSE"
