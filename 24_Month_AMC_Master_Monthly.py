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

response_log_str = '24_Month_AMC_Monthly_DataSet_response_' + str(today)
response_log = open('C:\scripts\dhis2\ETL\logs\\'+response_log_str,'w') #open the response log
Monthly_DataSet_file = 'C:\scripts\dhis2\ETL\logs\Monthly_Transaction_DataSet_'+str(today)+'.json' #create a log for the JSON payload
f = open(Monthly_DataSet_file,'w') #open the log for writing

storeIUDs = getStoreIDs()

print >> response_log, storeIUDs
print "Got store ID's and wrote to log\n"

#Calculate the 24 month AMC for each Item/orgUnit combo....rolling updated on the first of each month. end_date_24 is the last day of last month
url = "https://dhis2.co/msupply/api/analytics/events/aggregate/EgXN4cJ004K.json?stage=OiXz9SI5hGz&dimension=ou:"+storeIUDs+"&startDate="+start_date_24+"&endDate="+end_date_24+"&dimension=tnY9kPc9jTh&dimension=urAwnJWpKYj:eq:ci&value=y5gmjQRi54V&aggregationType=SUM" #define the DHIS2 API url for the events, org units will be identified via mSupply codes
headers = {'Content-type': 'application/json'} #define the payload as JSON
r = requests.get(url, headers=headers, auth=HTTPBasicAuth('kgbolger', 'Ireland15?'), verify=False) #POST with basic authentication
j = json.dumps(r.content) #dump to create the JSON from the dictonary
print >> response_log, url #write the JSON payload to the log
print >> response_log, j #write the JSON payload to the log

json_data = json.loads(r.content)
ids = []
dataValues = {}; 
dataValues["dataValues"] = [] 
print "ran and recieved aggregate data\n"
for node in json_data["rows"]:
	value = {};
	dataValues["dataValues"].append(value);

	value["dataElement"] = getAMCUID(node[0])
	value["period"] = this_month
	value["orgUnit"] = node[2]
	if node[3].endswith('.0'):
		node[3] = node[3][:-2]
		value["value"]  = str(int(node[3])/24)
	elif node[3].endswith('E4'):
		node[3] = int(node[3][:-2])
		node[3] = str((node[3]*10000)/24)
	elif node[3].endswith('E5'):
		node[3] = int(node[3][:-2])
		node[3] = str((node[3]*100000)/24)
		value["value"] = node[3][:-2]
	elif node[3].endswith('E6'):
		node[3] = int(node[3][:-2])
		node[3] = str((node[3]*1000000)/24)
		value["value"] = node[3][:-2]
	elif node[3].endswith('E7'):
		node[3] = float(node[3][:-2])
		node[3] = int((node[3]*10000000)/24)
		value["value"] = str(node[3])
	elif node[3].endswith('E8'):
		node[3] = int(node[3][:-2])
		node[3] = str((node[3]*100000000)/24)
		value["value"] = node[3][:-2]

j = json.dumps(dataValues) #dump to create the JSON from the dictonary
print >> f, j #write the JSON payload to the log
print "Agg JSON ready\n"
#This section handles the JSON POST
url = "https://dhis2.co/msupply/api/dataValueSets?importStrategy=CREATE_AND_UPDATE" #define the DHIS2 API url for the events, org units will be identified via mSupply codes
headers = {'Content-type': 'application/json'} #define the payload as JSON
r = requests.post(url, data=j, headers=headers, auth=HTTPBasicAuth('kgbolger', 'Ireland15?'), verify=False) #POST with basic authentication
print "Past hang!?\n"
json_data = json.loads(r.content) #read the JSON response
print json_data
print >> response_log, json_data #write the full response to the log
http_status = json_data['status'] #find the http status code of the POST operation
ignored_count = json_data['importCount']['ignored'] #find if any records were ignored
if http_status == "SUCCESS" and ignored_count == 0: #if the http status is 200 'OK' and nothing was ignored;
    print 'Success!!' #tell me it worked
    print >> response_log, 'Success!!'
