from flask import Flask, jsonify, request, url_for
from cassandra.cluster import Cluster
import uuid
import os

app = Flask(__name__)

cluster = Cluster([os.environ['CASSANDRA_HOST']])
session = cluster.connect(os.environ['CASSANDRA_KEYSPACE'])

@app.route('/', methods=['GET'])
def get_root():
    dictToReturn = {'status': 'running'}
    return jsonify(dictToReturn)

@app.route('/api/v1.0/status', methods=['GET'])
def get_status():
    rows = session.execute('SELECT uuid, status FROM queue')
    statusDict = dict()
    for status_row in rows:
        statusDict[status_row.uuid] = status_row.status
    return jsonify(statusDict)

@app.route('/api/v1.0/start', methods=['POST'])
def post_start():
    input_json = request.get_json(force=True) 

    print ('init from client', input_json)
    if input_json:
        session.execute(
        """
        INSERT INTO queue (UUID, parameters, hdfs, status)
        VALUES (%s, %s, %s, %s)
        """,
        (uuid.uuid1(), input_json, "", 0)
        )
        dictToReturn = {'status': 'ok'}
    else:
        dictToReturn = {'status': 'oh no'}
    return jsonify(dictToReturn)

if __name__ == '__main__':
    app.run(debug=True)