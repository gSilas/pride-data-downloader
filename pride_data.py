import sys
import json
import os
import csv
import gzip
import urllib
import shutil
import logging
import argparse
import requests


import json_writer
import csv_writer

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

handler = logging.FileHandler('debug.log', mode='w')
handler.setFormatter(logging.Formatter(
    fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
handler.setLevel(logging.DEBUG)
log.addHandler(handler)

handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter(
    fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
handler.setLevel(logging.INFO)
log.addHandler(handler)


def get_projectlist(args):

    project_list = []

    if args.accessions:
        for acc in args.accessions:
            url = "https://www.ebi.ac.uk:443/pride/ws/archive/project/" + ''.join(acc)

    else:
        urls = [] 
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

    for url in urls:
        log.info("Requested URL: %s", url)

        responseList = requests.get(url)
        
        if responseList:
            project_list = responseList.json()['list']
        else:
            log.error("No PRIDE server response received!")
            continue

        if args.submission:
            projectList += [project['accession'] for project in project_list if project['submissionType'] == args.submission]

        else:
            projectList += [project['accession'] for project in project_list]

    return projectList


def get_filelist(project):
    # Output project files

    files = dict()
    mgf_files = dict()
    mzid_files = dict()

    # Set the request URL
    url = 'https://www.ebi.ac.uk/pride/ws/archive/file/list/project/' + project
    # Request url and convert response to json
    try:
        project_files = requests.get(url).json()['list']
    except (json.decoder.JSONDecodeError, KeyError) as err:
        log.error(err)
        log.error("Request to {} could not be decoded!".format(url))
        log.error("Project at {} !".format(project))

    for pfile in project_files:
        if 'downloadLink' in pfile:
            if pfile['downloadLink'].endswith('.mgf.gz') and pfile['fileType'] == "PEAK":
                mgf_files[pfile["assayAccession"]] = pfile['downloadLink']
            elif pfile['downloadLink'].endswith('.mzid.gz') and pfile['fileType'] == "RESULT":
                mzid_files[pfile['assayAccession']] = pfile['downloadLink']

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


def download_projectlist(projects, folder, single_file=False):
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
                    if mgf:
                        extracted_mgf = extract_remove_file(mgf)
                        log.info("Downloaded: {} to {}".format(
                            mgf_file, extracted_mgf))
                    mzid = download_file(mzid_file, os.path.join(folder, key))
                    if mzid:
                        extracted_mzid = extract_remove_file(mzid)
                        log.info("Downloaded: {} to {}".format(
                            mzid_file, extracted_mzid))
                    if extracted_mgf and extracted_mzid:
                        downloaded_files.append(
                            (project, extracted_mgf, extracted_mzid))
                    if single_file:
                        break

    return downloaded_files


def write_archive_file(archivePath, files):
    with open(archivePath, 'a+') as fp:
        csvwriter = csv.writer(fp, delimiter=';')
        for f in files:
            csvwriter.writerow(list(f))
    return archivePath


if __name__ == "__main__":
    log.info("PRIDE download started!")

    parser = argparse.ArgumentParser(
        description="Download PRIDE projects and create Nocoy trainable csv!")
    parser.add_argument('-A', '--accessions', nargs='*', default=None,
                        type=list, help="Specify certain projects by accessions to download.")
    parser.add_argument('-N', '--number', metavar='1..10000', type=int, choices=range(
        1, 10001), default=1, help="Maximal number of projects with fitting metadata to include.")
    parser.add_argument('-P', '--pages', metavar='1..50', type=int, choices=range(
        1, 51), default=1, help="Maximal number of project pages to search.")
    parser.add_argument('-O', '--single_file', action='store_true', help="Only download a single file tuple for each available project!")
    parser.add_argument('-I', '--instruments', nargs='*', default=None,
                        type=str, help="MS/MS instruments used in projects. String used by PRIDE")
    parser.add_argument('-S', '--species', nargs='*', default=None,
                        type=str, help="Species evaluated in projects. NCBI Taxonomy ID")
    parser.add_argument('-F', '--folder', nargs='*', default="data_pride",
                        type=str, help="Folder containing downloaded data relative to the python script!")
    parser.add_argument('-Sub', '--submission', default="COMPLETE",
                        type=str, help="SubmissionType for projects.")
    args = parser.parse_args()

    projects = get_projectlist(args)
    log.info("Found {} matching projects!".format(len(projects)))
    log.debug(projects)

    archivePath = os.path.join(args.folder, 'archive')

    if os.path.exists(archivePath):
        with open(archivePath) as fp:
            for line in fp:
                for project in projects:
                    if project in line:
                        projects.remove(projects)

    if args.single_file:
        log.info('Only downloading single file tuples for each available project!')

    if projects:
        downloaded_files = download_projectlist(projects, args.folder, args.single_file)

        jsonPath = os.path.join(args.folder, 'psms.json')
        if downloaded_files:
            write_archive_file(archivePath, downloaded_files)

        # json_writer.writeJSONPSMSfromArchive(archivePath, jsonPath)
        # csv_writer.writeCSVPSMSfromArchive(archivePath)
