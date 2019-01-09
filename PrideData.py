import sys
import json
import re
import os
import gzip
import requests
import urllib
import shutil
import logging
import xml.etree.cElementTree as ET
import argparse

from MGFFile import MGFFile
from mzIdentMLFile import mzIdentMLFile
from mzIdentMLStat import parse_mzident

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

handler = logging.FileHandler('debug.log')
handler.setFormatter(logging.Formatter(
    fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
handler.setLevel(logging.DEBUG)
log.addHandler(handler)

handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter(
    fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
handler.setLevel(logging.INFO)
log.addHandler(handler)

def download_file(url, dest_folder, filename=None):
    orig_filename = url.split('/')[-1]

    if not os.path.exists(dest_folder):
        os.mkdir(dest_folder)
    if not os.path.exists(os.path.join(dest_folder, 'conversions.csv')):
        with open(os.path.join(dest_folder, 'conversions.csv'), 'a+') as f:
            f.write('INPUT' + ',' + 'OUTPUT' + '\n')

    # Create right filename with correct ending
    if filename:
        if orig_filename.endswith('.mgf.gz') and not filename.endswith('.mgf.gz'):
            filename += '.mgf.gz'
        elif orig_filename.endswith('.mzid.gz') and not filename.endswith('.mzid.gz'):
            filename += '.mzid.gz'
        with open(os.path.join(dest_folder, 'conversions.csv'), 'a+') as f:
            f.write(orig_filename + ',' + filename + '\n')
    else:
        filename = orig_filename

    # download file
    f = urllib.request.urlopen(url)
    data = f.read()

    # write file
    with open(os.path.join(dest_folder, filename), 'wb') as f:
        f.write(data)

    return os.path.join(dest_folder, filename)


def extract_remove_files(folder):
    os.chdir(folder)
    compressed_files = []
    extracted_files = []

    # extract gzip archives containing mzid, mgf
    for f in os.listdir():
        print("Checking:", f)

        # check for .gz ending
        if f.endswith('.gz'):
            compressed_files.append(f)
            print("Extracting:", f)

            # copy in to out
            with gzip.open(f, 'rb') as f_in:
                with open(f[:-3], 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    extracted_files.append(f[:-3])
        print('\n')

    # Remove compressed files
    for f in compressed_files:
        os.remove(f)

    return extracted_files

def extract_remove_file(f):
    compressed_file = None
    extracted_file = None

    # extract gzip archives containing mzid, mgf
    print("Checking:", f)

    # check for .gz ending
    if f.endswith('.gz'):
        compressed_file = f
        print("Extracting:", f)

        # copy in to out
        with gzip.open(f, 'rb') as f_in:
            with open(f[:-3], 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                extracted_file = f[:-3]
    print('\n')

    # Remove compressed files
    os.remove(compressed_file)

    return extracted_file

def get_projectlist(args):

    project_list = []

    if args.accessions:
        for acc in args.accessions:
            url = "https://www.ebi.ac.uk:443/pride/ws/archive/project/" + acc
            project_list.append(requests.get(url).json())

    else:
        url = 'https://www.ebi.ac.uk:443/pride/ws/archive/project/list/?show=' + \
            str(args.number) + '&page=0&order=desc'
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
        log.info("Requested URL: %s", url)
        project_list = requests.get(url).json()['list']


    if args.submission:
        return [project['accession'] for project in project_list if project['submissionType'] == args.submission]
    else:
        return [project['accession'] for project in project_list]


def get_filelist(projects):
    # Output project files

    files = dict()

    for project in projects:
        mgf_files = dict()
        mzid_files = dict()

        # Set the request URL
        url = 'https://www.ebi.ac.uk/pride/ws/archive/file/list/project/' + project
        # Request url and convert response to json
        try:
            project_files = requests.get(url).json()['list']
        except json.decoder.JSONDecodeError:
            log.error("Request to {} could not be decoded!".format(url))

        for pfile in project_files:
            if 'downloadLink' in pfile:
                if pfile['downloadLink'].endswith('.mgf.gz') and pfile['fileType'] == "PEAK":
                    mgf_files[pfile["assayAccession"]] = pfile['downloadLink']
                elif pfile['downloadLink'].endswith('.mzid.gz') and pfile['fileType'] == "RESULT":
                    mzid_files[pfile['assayAccession']] = pfile['downloadLink']

        key_intersection = mgf_files.keys() & mzid_files.keys()

        pfiles = []

        for key in key_intersection:
            pfiles.append((mgf_files[key], mzid_files[key]))

        if pfiles:
            files[project] = pfiles

    return files

def download_files(folder, projects):
    file_name = ''
    files = get_filelist(projects)

    if not os.path.exists(folder):
        os.mkdir(folder)

    # download mzid and mgf files fro PRIDE
    id = 0
    print(files)
    for project in files:
        for mgf_file, mzid_file in files[project]:
            log.info("Downloaded: {} to {}".format(mgf_file, download_file(mgf_file, folder, str(id))))
            log.info("Downloaded: {} to {}".format(mzid_file, download_file(mzid_file, folder, str(id))))
            id += 1
            break

    return extract_remove_files(folder)


if __name__ == "__main__":
    log.info("PRIDE download started!")

    parser = argparse.ArgumentParser(
        description="Download PRIDE projects and create Nocoy trainable csv!")
    parser.add_argument('-A', '--accessions', nargs='*', default=None,
                        type=list, help="Specify certain projects by accessions to download.")
    parser.add_argument('-N', '--number', metavar='1..10000', type=int, choices=range(
        1, 10001), default=1, help="Maximal number of projects with fitting metadata to include.")
    parser.add_argument('-I', '--instruments', nargs='*', default=None,
                        type=str, help="MS/MS instruments used in projects. String used by PRIDE")
    parser.add_argument('-S', '--species', nargs='*', default=None,
                        type=str, help="Species evaluated in projects. NCBI Taxonomy ID")
    parser.add_argument('-Sub', '--submission', default="COMPLETE",
                        type=str, help="SubmissionType for projects.")
    args = parser.parse_args()

    projects = get_projectlist(args)
    log.info("Found {} matching projects!".format(len(projects)))
    log.debug(projects)

    downloaded_files = []
    id = 0
    folder = 'data_pride'
    for project in projects:
        files = get_filelist(project)
        log.info("{} projects contained files that can be processed!".format(len(files.keys())))
        log.debug(files)
        for project in files:
            for mgf_file, mzid_file in files[project]:
                if not os.path.exists(folder):
                    os.mkdir(folder)
                log.info("Downloaded: {} to {}".format(mgf_file, downloaded_files.append(extract_remove_file(download_file(mgf_file, folder, str(id))))))
                log.info("Downloaded: {} to {}".format(mzid_file, downloaded_files.append(extract_remove_file(download_file(mzid_file, folder, str(id))))))
                id += 1
                break

    # for files in downloaded_files:
    #     log.info("Analyzing {}".format(files))
    #     if files.split('.')[1] == 'mgf':
    #         parsed_mgf = MGFParser()
    #         parsed_mgf.parse_mgf(files)
    #         for key in parsed_mgf.entries:
    #             print(key)
    #             print(repr(parsed_mgf.entries[key]))
    #         print(parsed_mgf.tokens)

    #     elif files.split('.')[1] == 'mzid':
    #         parsed_mzid = mzIdentMLParser()
    #         parsed_mzid.parse_mzident(files)
    #         for key in parsed_mzid.entries:
    #             print(key)
    #             print(repr(parsed_mzid.entries[key]))
    out = []
    count = []

    for files in downloaded_files:
        if files.split('.')[1] == 'mzid':
            try:
                tup = parse_mzident(files)
                if tup not in out:
                    out.append(tup)
                    count.append(1)
                else:
                    count[out.index(tup)] += 1
            except (xml.etree.ElementTree.ParseError, ValueError) as err:
                print("File: " + str(files) + " is bad!")
                print(err)
                print(err.args)

    print(out)
    print(count)

    #completed_mgfs = write_completed_mgf(downloaded_files)

    #files = merge_mgfs(completed_mgfs, 0.3)
