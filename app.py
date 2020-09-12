import logging
import argparse
import sys
import os
import json
import time
import datetime

from accessors.pride_data import get_filelist, get_projectlist, write_archive_file, download_projectlist

from writers import mgf_writer

log = logging.getLogger('PrideData')
log.setLevel(logging.DEBUG)

if not os.path.exists("logs/"):
    os.mkdir("logs")

log_filename = 'logs/logging-{}.log'.format(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
handler = logging.FileHandler(log_filename, mode='w')
handler.setFormatter(logging.Formatter(
    fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
handler.setLevel(logging.DEBUG)
log.addHandler(handler)

handler = logging.StreamHandler(stream=sys.stdout)
handler.setFormatter(logging.Formatter(
    fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
handler.setLevel(logging.INFO)
log.addHandler(handler)

if __name__ == "__main__":
    log.info("PRIDE download started!")

    parser = argparse.ArgumentParser(
        description="Download PRIDE projects and create Nocoy trainable csv!")
    parser.add_argument('-A', '--accessions', nargs='*', default=None,
                        type=list, help="Specify certain projects by accessions to download.")
    parser.add_argument('-M', '--memory', type=float, default=1.0, help="Limits the RAM for the program to the given ratio of the available RAM!")
    parser.add_argument('-N', '--number', metavar='1..10000', type=int, choices=range(
        1, 10001), default=1, help="Maximal number of projects per page with fitting metadata to include.")
    parser.add_argument('-P', '--pages', metavar='1..50', type=int, choices=range(
        1, 51), default=1, help="Maximal number of project pages to search.")
    parser.add_argument('-INI', '--ini',  nargs='*',  default=None, help="Disregard command line arguments and parse configuration from a given config file!")
    parser.add_argument('-I', '--instruments', nargs='*', default=None,
                        type=str, help="MS/MS instruments used in projects. String used by PRIDE")
    parser.add_argument('-S', '--species', nargs='*', default=None,
                        type=str, help="Species evaluated in projects. NCBI Taxonomy ID")
    parser.add_argument('-F', '--folder', nargs='*', default="data_pride",
                        type=str, help="Folder containing downloaded data relative to the python script!")
    parser.add_argument('-SUB', '--submission', default="COMPLETE",
                        type=str, help="SubmissionType for projects.")
    parser.add_argument('-CO', '--cores', default=4, type=int, help="Maximal number of cores!")
    parser.add_argument('-PR', '--process', default=None, type=str, 
                        help="archive file")

    args = parser.parse_args()

    if args.process: 
        mgf_writer.writeMGFSfromArchive(args.folder, args.process, args.cores)

    else:
        if args.ini:
            argparse_dict = vars(args)
            log.info("Parsing configuration from {}".format(args.ini[0]))
            with open(args.ini[0], 'r') as configfile:
                argparse_dict.update(json.load(configfile))

        print(args)

        projects, projectDescriptions = get_projectlist(args)
        log.info("Found {} matching projects!".format(len(projects)))
        log.debug(projects)

        archivePath = os.path.join(args.folder, 'archive')

        if os.path.exists(archivePath):
            with open(archivePath) as fp:
                for line in fp:
                    for project in projects:
                        if project in line:
                            projects.remove(project)

        if projects:
            downloaded_files = download_projectlist(projects, projectDescriptions, args.folder)

            if downloaded_files:
                write_archive_file(archivePath, downloaded_files)
