#!/usr/bin/env python

import os
import pyodbc
import json
import collections
import subprocess
import sys
import datetime 
from datetime import timedelta
import requests
import ConfigParser
from requests.auth import HTTPBasicAuth
from Master_functions import getAMC, setType, monthdelta, dataValue, getStoreUID,getOpeningBalUID,getClosingBalUID,getReceivedBalUID,getIssuedBalUID,getAMCUID,getUserAMCUID, getStoreIDs

today = datetime.date.today()
first = today.replace(day=1)
this_month = today.strftime("%Y%m")
today = today.strftime("%Y%m%d")
last_month = (first - datetime.timedelta(days=1)).strftime("%Y%m")
twenty4_months = (first - datetime.timedelta(days=728))
six_months = (first - datetime.timedelta(days=182))
start_date_24 = twenty4_months.replace(day=1).strftime("%Y-%m-%d")
end_date_24 = (first - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
start_date_6 = six_months.replace(day=1).strftime("%Y-%m-%d")
end_date_6 = (first - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
last = (first - datetime.timedelta(days=1)).strftime("%Y%m%d")
first = str(first.strftime("%Y%m%d"))
last = str(last)

response_log_str = 'Monthly_CI_Transaction_DataSet_response_' + str(today)
response_log = open('C:\scripts\dhis2\ETL\logs\\'+response_log_str,'w') #open the response log
Monthly_DataSet_file = 'c:\scripts\dhis2\ETL\logs\Monthly_CI_Transaction_DataSet_data_'+str(today)+'.json' #create a log for the JSON payload
f = open(Monthly_DataSet_file,'w') #open the log for writing

storeIUDs = getStoreIDs()

print >> response_log, storeIUDs
print "Got store ID's and wrote to log\n"
#Get Last months CI totals (aggreate SUM of transaction quantity for transaction type CI)
url = "https://dhis2.co/msupply/api/analytics/events/aggregate/EgXN4cJ004K.json?stage=OiXz9SI5hGz&dimension=ou:"+storeIUDs+"&dimension=pe:LAST_MONTH&dimension=tnY9kPc9jTh&dimension=urAwnJWpKYj:eq:ci&value=y5gmjQRi54V&aggregationType=SUM" #define the DHIS2 API url for the events, org units will be identified via mSupply codes
headers = {'Content-type': 'application/json'} #define the payload as JSON
r = requests.get(url, headers=headers, auth=HTTPBasicAuth('kgbolger', 'Ireland15?'), verify=False) #POST with basic authentication

json_data = json.loads(r.content)
ids = []
dataValues = {}; 
dataValues["dataValues"] = [] 

for node in json_data["rows"]:
	value = {};
	dataValues["dataValues"].append(value);

	value["dataElement"] = getIssuedBalUID(node[0])
	value["period"] = last_month
	value["orgUnit"] = node[2]
	if node[4].endswith('.0'):
		value["value"] = node[4][:-2]
	elif node[4].endswith('E4'):
		node[4] = float(node[4][:-2])
		node[4] = str(node[4]*10000)
	elif node[4].endswith('E5'):
		node[4] = float(node[4][:-2])
		node[4] = str(node[4]*100000)
		value["value"] = node[4][:-2]
	elif node[4].endswith('E6'):
		node[4] = float(node[4][:-2])
		node[4] = str(node[4]*1000000)
		value["value"] = node[4][:-2]
	elif node[4].endswith('E7'):
		node[4] = float(node[4][:-2])
		node[4] = str(node[4]*10000000)
		value["value"] = node[4][:-2]
	elif node[4].endswith('E8'):
		node[4] = float(node[4][:-2])
		node[4] = str(node[4]*100000000)
		value["value"] = node[4][:-2]

j = json.dumps(dataValues) #dump to create the JSON from the dictonary
print >> f, j #write the JSON payload to the log

#This section handles the JSON POST
url = "https://dhis2.co/msupply/api/dataValueSets?importStrategy=CREATE_AND_UPDATE&dryRun=FALSE" #define the DHIS2 API url for the events, org units will be identified via mSupply codes
headers = {'Content-type': 'application/json'} #define the payload as JSON
r = requests.post(url, data=j, headers=headers, auth=HTTPBasicAuth('kgbolger', 'Ireland15?'),verify=False) #POST with basic authentication

json_data = json.loads(r.content) #read the JSON response
print json_data
print >> response_log, json_data #write the full response to the log
http_status = json_data['status'] #find the http status code of the POST operation
ignored_count = json_data['importCount']['ignored'] #find if any records were ignored
if http_status == "SUCCESS" and ignored_count == 0: #if the http status is 200 'OK' and nothing was ignored;
    print 'Success!!' #tell me it worked
    print >> response_log, "Success!!"
