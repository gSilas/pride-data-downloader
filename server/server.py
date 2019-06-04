from flask import Flask, jsonify, request, url_for
from gevent.pywsgi import WSGIServer
from cassandra.cluster import Cluster
import uuid
import os
import json

app = Flask(__name__)

cluster = Cluster(os.environ['CASSANDRA_HOST'].split())
session = cluster.connect(os.environ['CASSANDRA_KEYSPACE'])

@app.route('/', methods=['GET'])
def get_root():
    dictToReturn = {'status': 'running'}
    return jsonify(dictToReturn)

@app.route('/status', methods=['GET'])
def get_status():
    rows = session.execute('SELECT CSV_GENERATOR_JOB_ID, STATUS FROM CSV_GENERATOR_JOBS')
    statusDict = dict()
    for status_row in rows:
        statusDict[str(status_row.csv_generator_job_id)] = status_row.status
    return jsonify(statusDict)

@app.route('/start', methods=['POST'])
def post_start():
    input_json = request.get_json(force=True) 

    print ('init from client', input_json)
    if input_json:
        session.execute(
        """
        INSERT INTO CSV_GENERATOR_JOBS (CSV_GENERATOR_JOB_ID, PRIDE_ID, CSV_HDFS_PATH, STATUS, PARAMETER, JOB_RESULT_MESSAGE, PRIDE_PROJECT_INFORMATION)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (uuid.uuid1(), "", "", 0, str(json.dumps(input_json)), "", "")
        )
        dictToReturn = {'status': 'inserted input'}
    else:
        dictToReturn = {'status': 'failed to insert input'}
    return jsonify(dictToReturn)

if __name__ == '__main__':  
    http_server = WSGIServer(('', 5000), app)
    http_server.serve_forever()