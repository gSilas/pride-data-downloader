import logging
import argparse
import sys
import os
import ast
import shutil
from subprocess import Popen, PIPE
from argparse import Namespace

from writers import csv_writer
from writers import json_writer

from utils import get_memory
from utils import memory_limit

import time
import datetime
from accessors.pride_data import get_filelist, get_projectlist, write_archive_file, download_projectlist

from hdfs import InsecureClient
from cassandra.cluster import Cluster
import uuid

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

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
    log.info("PRIDE download daemon started!")

    cluster = Cluster(os.environ['CASSANDRA_HOST'].split())
    session = cluster.connect(os.environ['CASSANDRA_KEYSPACE'])

    while(True):
        log.info('Executing daemon loop!')
        csvs = None
        error = None
        rows = session.execute('SELECT * FROM CSV_GENERATOR_JOBS WHERE STATUS=0 ALLOW FILTERING;')
        for row in rows:
            try:
                log.info("Current job: {}".format(row.csv_generator_job_id))
                json_data = ast.literal_eval(str(row.parameter))
                #print(json_data)
                args = Namespace(**json_data)
                csvs = None

                memory_limit(args.memory)
                projects, projectDescriptions = get_projectlist(args)
                log.info("Found {} matching projects!".format(len(projects)))
                log.debug(projects)

                archivePath = os.path.join("data_pride", 'archive')

                if os.path.exists(archivePath):
                    log.info('Archive exists! Deleting archived data!')
                    os.remove(archivePath)

                if not os.path.exists(args.folder):
                    log.info('Folder does not exists! Creating {}!'.format(args.folder))
                    os.makedirs(args.folder)

                if args.single_file:
                    log.info('Only downloading single file tuples for each available project!')

                if projects:
                    log.info("Downloading files {}".format(projects))
                    downloaded_files = download_projectlist(projects, projectDescriptions, args.folder, args.single_file)

                    jsonPath = os.path.join(args.folder, 'psms.json')
                    
                    if downloaded_files:
                        write_archive_file(archivePath, downloaded_files)

                        if args.csv:
                            csvs = csv_writer.writeCSVPSMSfromArchive(archivePath, args.cores, args.features)

                        if args.json:
                            json_writer.writeJSONPSMSfromArchive(archivePath, jsonPath)
                    
                    else:
                        log.warning("No files downloaded!")
            
            except Exception as err:
                log.error("Exception {}".format(err)) 
                error = err

            if csvs:
                log.info("CSVs generated during execution!")
                hdfs_parent = os.path.join(os.sep, os.environ['HDFS_USER'], str(row.csv_generator_job_id))
                for csv in csvs:
                    hdfs_path = os.path.join(hdfs_parent, csv.split(os.sep)[-1])

                    # put csv into hdfs
                    #put = Popen(["hadoop", "fs", "-put", csv, hdfs_path], stdin=PIPE, bufsize=-1)
                    #put.communicate()

                    client = InsecureClient('{}:{}'.format(os.environ['HDFS_HOST'], os.environ['HDFS_PORT']), user=os.environ['HDFS_USER'])
                    client.upload(hdfs_path, csv)

                session.execute("UPDATE CSV_GENERATOR_JOBS SET STATUS = 1, CSV_HDFS_PATH={}, PRIDE_ID={}, JOB_RESULT_MESSAGE=\'{}\' WHERE CSV_GENERATOR_JOB_ID={} IF EXISTS;".format(hdfs_parent, projects, "success", str(row.csv_generator_job_id)))
            else:
                log.warning("No CSVs generated during execution!")
                session.execute("UPDATE CSV_GENERATOR_JOBS SET STATUS = -1, JOB_RESULT_MESSAGE=\'{}\' WHERE CSV_GENERATOR_JOB_ID={} IF EXISTS;".format(str(error), str(row.csv_generator_job_id)))

        log.info('Sleeping for {} seconds!'.format(os.environ['TIMEOUT']))
        time.sleep(int(os.environ['TIMEOUT']))