def class_label(mzid):
    """
    Inferes a class label based on predefined conditions.
    
    Parameters
    ----------
    mzid: _Result
        mzid result object
    
    Returns
    -------
    tuple
        Contains the label and the rule used for the label decision.
    """

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
                return 'scaffold', 'true'
            elif float(param[1]) < 0.99:
                return 'scaffold', 'false'

    if mascot_score != -999 and mascot_threshold != -999:
        if mascot_score >= mascot_threshold:
            return 'mascot', 'true'
        elif mascot_score < mascot_threshold:
            return 'mascot', 'false'

    if int(mzid.rank) == 1:
        return 'rank', 'true'
    elif int(mzid.rank) > 1:
        return 'rank', 'false'
