from flask import Flask, flash, redirect, url_for, request, get_flashed_messages
from pymongo import MongoClient
import json
from bson import json_util
import pycurl, json
from flask.ext.cors import CORS, cross_origin
import cStringIO

app = Flask(__name__)
cors = CORS(app)

#Creating dependency objects
client = MongoClient(host="127.0.0.1")
db = client.WK


# use for encrypt session
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route('/')
def index():
    return (
        '''
            Welcome !!! WK API
        '''
    )


@app.route('/getTablesInfo')
@cross_origin()
def getTablesInfo():
    cursor = db.tables.find()
    json_docs = []
    for doc in cursor:
        json_str = json.dumps(doc, default=json_util.default)
        json_docs.append(json_str)
    return json.dumps(json_docs)

@app.route('/postJob', methods = ['POST'])
@cross_origin()
def runSparkJob():
	payload = request.get_json()
	jobResponse = cStringIO.StringIO()
	curlreq = pycurl.Curl()
	print json.dumps(payload)
	url = 'http://172.16.248.156:8090/jobs?appName=final&classPath=DataChecks.basicStats'
	curlreq.setopt(pycurl.URL, url)
	curlreq.setopt(pycurl.HTTPHEADER, ['Accept: application/json'])
	curlreq.setopt(pycurl.POST, 1)
	curlreq.setopt(curlreq.WRITEFUNCTION, jobResponse.write)
	curlreq.setopt(pycurl.POSTFIELDS, json.dumps(payload))
	curlreq.perform()
	js = json.loads(json.dumps(payload))
	curlreq.close()
	js['response'] = json.loads(jobResponse.getvalue())
	js['jobId'] = json.loads(jobResponse.getvalue())['result']['jobId']
	db.configs.save(js)
	print jobResponse.getvalue()
	return jobResponse.getvalue()

@app.route('/getResults', methods = ['POST'])
@cross_origin()
def getResults():
	jobResponse = cStringIO.StringIO()
	jobIds = request.get_json()
	for jobId in jobIds:
		url = 'http://172.16.248.156:8090/jobs/' + jobId
		curlreq = pycurl.Curl()
		curlreq.setopt(pycurl.URL, url)
#		curlreq.setopt(pycurl.HTTPHEADER, ['Accept: application/json'])
		curlreq.setopt(curlreq.WRITEFUNCTION, jobResponse.write)
		curlreq.perform()
		response = json.loads(jobResponse.getvalue())
		curlreq.close()
		print response['status']
		if response['status'] == "FINISHED" or response['status'] == "ERROR":
			db.configs.update({'jobId':jobId}, {'$set': {'response': response}})
	result = listConfigs()
	return result

@app.route('/listConfigs')
@cross_origin()
def listConfigs():
	cursor = db.configs.find()
	json_docs = []
	for doc in cursor:
		json_str = json.dumps(doc, default=json_util.default)
		json_docs.append(json_str)
	return json.dumps(json_docs)

#@app.route('/addConfigs', methods = ['POST'])
#@cross_origin()
#def addConfigs():
#	config = request.get_json()
#	db.configs.save(config)
#	return "True"

@app.route('/getConfig', methods = ['POST'])
@cross_origin()
def getConfig():
	configName = request.get_json()
	cursor = db.configs.find({'configName': configName})
	json_docs = []
	for doc in cursor:
		json_str = json.dumps(doc, default=json_util.default)
		json_docs.append(json_str)
	return json.dumps(json_docs)

#@app.route('/saveResults')
#@cross_origin()
#def saveResults():
#	return "True"


if __name__ == '__main__':
    app.run(host = "0.0.0.0", port = 9999, debug = True)