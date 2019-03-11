def class_label(mzid):

    mascot_score = -999
    mascot_threshold = -999

   # print(mzid.parameters)
    for param in mzid.parameters:
        if 'Mascot:identity threshold' == param[0]:
            mascot_threshold = param[1]

        elif 'Mascot:score' == param[0]:
            mascot_score = param[1]

        elif 'Scaffold:Peptide Probability' == param[0]:
            if float(param[1]) >= 0.99:
                return 'scaffold', "TRUE"
            else:
                return 'scaffold', "FALSE"

    if mascot_score != -999 and mascot_threshold != -999:
        if mascot_score > mascot_threshold:
            return 'mascot', "TRUE"
        else:
            return 'mascot', "FALSE"

    elif int(mzid.rank) == 1:
        return 'rank', "TRUE"
    elif int(mzid.rank) > 1:
        return 'rank', "FALSE"
