import re

def generate_mgf_list(input_file):
    """ 
    Generates List from MGF 
    
    Parameters
    ----------
    input_file: str
        path to mgf file

    Returns
    -------
    list
        lines in an mgf file

    """ 
    with open(input_file, 'r') as mgf_file:
        mgf_list = mgf_file.read().split('\n')
    return mgf_list


def get_mgf_tokens(mgf_list):
    """ 
    Calculates tokens from MGF-list 
    
    Parameters
    ----------
    mgf_list: list
        lines of an mgf

    Returns
    -------
    dict
        tokens in an mgf
    
    """
    index = 0
    tokens = dict()
    line = mgf_list[index]
    while not 'END IONS' in line:
        if '=' in line:
            tokens[line.split('=')[0]] = index
        if re.match(r'\d+\.\d+\t\d+\.\d+', line):
            tokens['LISTS'] = index
            break
        index += 1
        line = mgf_list[index]
    return tokens


def parse_mgf(input_file):
    """ 
    Parses MGF by calculating offsets and indexing individual lines in an MGF 
    
    Parameters
    ----------
    input_file : str
        path to an mgf

    Returns
    -------
    tuple
        entries in the mgf and tokens of the mgf

    """

    entries = dict()
    mgf_list = generate_mgf_list(input_file)
    tokens = get_mgf_tokens(mgf_list)

    index = 0
    spectrum_mgf_index = 0
    line = mgf_list[index]

    while index < len(mgf_list):
        line = mgf_list[index]

        if 'BEGIN IONS' in line:

            attributes = dict()
            try:
                line = mgf_list[index + tokens['TITLE']]
                attributes['title'] = re.sub(r'TITLE=', '', line)

                line = mgf_list[index + tokens['PEPMASS']]
                attributes['pepmass'] = float(re.sub(r'PEPMASS=', '', line))

                line = mgf_list[index + tokens['CHARGE']]
                attributes['charge'] = int(re.match(r'CHARGE=(\d)\+', line)[1] if re.match(r'CHARGE=(\d)\+', line) else 0)
                
                spectrum_index = index + tokens['LISTS']
                line = mgf_list[spectrum_index]
                mz_lst = []
                intensity_lst = []
            except KeyError:
                return dict(), list()

            while not 'END IONS' in line:
                line_split = line.split('\t')
                mz_lst.append(float(line_split[0].strip()))
                intensity_lst.append(float(line_split[1].strip()))
                spectrum_index += 1
                line = mgf_list[spectrum_index]

            attributes['mz_list'] = mz_lst
            attributes['intensity_list'] = intensity_lst

            entries['index='+str(spectrum_mgf_index)] = attributes

            spectrum_mgf_index += 1

        index += 1

    return entries, tokens


if __name__ == '__main__':
    name = input("MGF File:")
    mgf_zero, tokens = parse_mgf(name)
    for key in mgf_zero:
        print(key, repr(mgf_zero[key]))
    print(tokens)
