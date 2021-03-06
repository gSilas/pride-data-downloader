import sys
import json
import os
import csv
import gzip
import urllib
import shutil
import logging
import requests
import time
import datetime

log = logging.getLogger('PrideData')

def get_projectlist(args):
    """
    Returns a projectlist containing all valid project accessions.
    
    Parameters
    ----------
    args : list
        programm execution arguments

    Returns
    -------
    tuple(list, dict)
        tuple containing list of projects and dict of matched project descriptions 
        

    """

    project_list = []
    urls = []

    if args.accessions:
        for acc in args.accessions:
            url = "https://www.ebi.ac.uk:443/pride/ws/archive/project/" + ''.join(acc)
            urls.append(url)

    else:
        for page in range(0, args.pages):
            url = 'https://www.ebi.ac.uk:443/pride/ws/archive/project/list/?show=' + \
                str(args.number) + '&page=' + str(page) + '&order=desc'
            if args.species:
                url += '&speciesFilter='
                for spec in args.species:
                    url += spec + '%2C%20'
                url = url[:-6]
            if args.instruments:
                url += '&instrumentFilter='
                for ins in args.instruments:
                    url += ins + '%2C%20'
                url = url[:-6]
            urls.append(url)

    projectList = []
    projectDescriptions = dict()

    modifications = dict()
    with open('config/modifications.csv', 'r') as fp:
        modification_csv = csv.reader(fp, delimiter=';')
        for row in modification_csv:
            modifications[row[0]] = True if not '1' in row[1] else False

    for url in urls:
        log.info("Requested URL: %s", url)

        responseList = requests.get(url)
        
        if responseList and not args.accessions:
            project_list = responseList.json()['list']
        elif args.accessions:
            project_list = [responseList.json()]
        else:
            log.error("No PRIDE server response received!")
            continue

        for project in project_list:
            unsupported_mod = False

            for mod in project['ptmNames']:
                if not mod in modifications:
                    log.warning("Unsupported modification! {} Skipping this project!".format(mod))
                    unsupported_mod = True
                    break
                else:
                    if not modifications[mod]:
                        log.warning("Unsupported modification! {} Skipping this project!".format(mod))
                        unsupported_mod = True
                        break

            if unsupported_mod:
                continue

            if args.submission:
                if project['submissionType'] == args.submission:
                    projectList += [project['accession']]
                    projectDescriptions[project['accession']] = project

            else:
                projectList += [project['accession']]
                projectDescriptions[project['accession']] = project

    return projectList, projectDescriptions


def get_filelist(project):
    """ 
    Output project file tuples.

    Parameters
    ----------
    project : str
        PRIDE project acession ID

    Returns
    -------
    list
        Files associated with a project
    
    """

    files = dict()
    mgf_files = dict()
    mzid_files = dict()

    # Set the request URL
    url = 'https://www.ebi.ac.uk/pride/ws/archive/file/list/project/' + project
    # Request url and convert response to json
    try:
        log.info("Requesting filelist at {} !".format(url))
        project_files = requests.get(url).json()['list']
    except (json.decoder.JSONDecodeError, KeyError) as err:
        log.error(err)
        log.error("Request to {} could not be decoded!".format(url))
        log.error("Project at {} !".format(project))
        return None

    for pfile in project_files:
        if 'downloadLink' in pfile:
            if pfile['downloadLink'].endswith('.mgf.gz') and pfile['fileType'] == "PEAK" and pfile['fileSource'] == "GENERATED":
                mgf_files[pfile["projectAccession"] + pfile["assayAccession"]] = pfile['downloadLink']
            elif pfile['downloadLink'].endswith('.mzid.gz') and pfile['fileType'] == "RESULT":
                mzid_files[pfile["projectAccession"] + pfile["assayAccession"]] = pfile['downloadLink']

    key_intersection = mgf_files.keys() & mzid_files.keys()

    if not key_intersection:
        log.info('{} contained no files that can be processed!'.format(project))

    pfiles = []

    for key in key_intersection:
        pfiles.append((mgf_files[key], mzid_files[key]))

    if pfiles:
        files[project] = pfiles

    return files


def download_file(url, dest_folder):
    """ 
    Downloads a file.
    
    Parameters
    ----------
    url : str
        url to a downloadable file.
    dest_folder : str
        path to a destination for the downloaded file.

    Returns
    -------
    str
        path to the downloaded file

    """
    orig_filename = url.split('/')[-1]

    if not os.path.exists(dest_folder):
        os.mkdir(dest_folder)

    # download file
    
    try:
        f = urllib.request.urlopen(url)
    except urllib.error.URLError as err:
        log.error('Error on request for file: {} error: {}'.format(url, err))
        return None

    data = f.read()

    # write file
    with open(os.path.join(dest_folder, orig_filename), 'wb') as f:
        f.write(data)

    return os.path.join(dest_folder, orig_filename)


def extract_remove_file(f):
    """ 
    Extracts and removes gzipped file.

    Parameters
    ----------
    f : str
        Path to a gzipped file.

    Returns
    -------
    str
        Path of extracted file.

    """
    compressed_file = None
    extracted_file = None

    # extract gzip archives containing mzid, mgf
    log.info("Checking: {}".format(f))

    # check for .gz ending
    if f.endswith('.gz'):
        compressed_file = f
        log.info("Extracting: {}".format(f))

        # copy in to out
        with gzip.open(f, 'rb') as f_in:
            with open(f[:-3], 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                extracted_file = f[:-3]

    # Remove compressed files
    os.remove(compressed_file)

    return extracted_file


def download_projectlist(projects, projectDescriptions, folder, single_file=False):
    """ 
    Downloads a list of file tuples to a destination folder.
    
    Parameters
    ----------
    projects: list
        List of file tuples with ftp links.
    projectDescriptions: dict
        Dictionary containing the descriptions of projects per accession.
    folder: str
        Destination folder of the downloads
    single_file: bool
        stops execution after donwloading a single file per project
    
    Returns
    -------
    list
        List of downloaded files.
    
    """
    if projects:
        if not os.path.exists(folder):
            os.mkdir(folder)

    downloaded_files = []

    for project in projects:
        if project in os.listdir(folder):
            continue

        files = get_filelist(project)
        if files:
            log.info(
                "Attempting to download {}!".format(project))
            log.debug(files)
            for key in files:

                for mgf_file, mzid_file in files[key]:
                    mgf = download_file(mgf_file, os.path.join(folder, key))
                    mzid = download_file(mzid_file, os.path.join(folder, key))

                    if mgf and mzid:
                        extracted_mgf = extract_remove_file(mgf)
                        log.info("Downloaded: {} to {}".format(
                            mgf_file, extracted_mgf))
                        extracted_mzid = extract_remove_file(mzid)
                        log.info("Downloaded: {} to {}".format(
                            mzid_file, extracted_mzid))
                        if extracted_mgf and extracted_mzid:
                            downloaded_files.append((project, extracted_mgf, extracted_mzid))
                    else:
                        log.warning("No generated tuple: Removing files {}".format((mgf, mzid)))
                        if os.path.exists(str(mzid)):
                            os.remove(mzid)
                        if os.path.exists(str(mgf)):
                            os.remove(mgf)

                    if single_file:
                        break
                    
                with open(os.path.join(folder, key, 'report.json'), 'w') as jsonFile:
                        json.dump(fp=jsonFile, obj=projectDescriptions[project])

    return downloaded_files


def write_archive_file(archivePath, files):
    """ 
    Writes archive file containing downloaded and extracted project file tuples! 
    
    Parameters
    ----------
    archivePath: str
        Path to the generated archive file.
    files: list
        Unordered List of the downloaded and extracted file tuples.
    
    Returns
    -------
    str
        Path of generated archive file.

    """
    with open(archivePath, 'a+') as fp:
        csvwriter = csv.writer(fp, delimiter=';')
        for f in files:
            csvwriter.writerow(list(f))
    return archivePath


