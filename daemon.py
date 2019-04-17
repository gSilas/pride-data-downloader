import logging
import argparse
import sys
import os
import json

from argparse import Namespace

from writers import csv_writer
from writers import json_writer

from utils import get_memory
from utils import memory_limit

from time import sleep

from accessors.pride_data import get_filelist, get_projectlist, write_archive_file, download_projectlist

from cassandra.cluster import Cluster
import uuid
import os

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

if __name__ == "__main__":
    log.info("PRIDE download daemon started!")

    cluster = Cluster([os.environ['CASSANDRA_HOST']])
    session = cluster.connect(os.environ['CASSANDRA_KEYSPACE'])

    while(True):
        rows = session.execute('SELECT parameters, status FROM queue WHERE status=0')
        for row in rows:
            args = Namespace(**json.loads(row.parameters))
            print(args)

            sys.exit()

            memory_limit(args.memory)
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

            if args.single_file:
                log.info('Only downloading single file tuples for each available project!')

            if projects:
                downloaded_files = download_projectlist(projects, projectDescriptions, args.folder, args.single_file)

                jsonPath = os.path.join(args.folder, 'psms.json')
                if downloaded_files:
                    write_archive_file(archivePath, downloaded_files)

            if args.csv:
                csv_writer.writeCSVPSMSfromArchive(archivePath, args.cores)

            if args.json:
                json_writer.writeJSONPSMSfromArchive(archivePath, jsonPath)
        
        sleep(360)