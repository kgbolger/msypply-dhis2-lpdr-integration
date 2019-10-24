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
from Master_functions import getAMC, setType, monthdelta, dataValue, getStoreUID,getOpeningBalUID,getClosingBalUID,getStoreIDs

today = datetime.date.today()
first = today.replace(day=1)
this_month = today.strftime("%Y%m")
today = today.strftime("%Y%m%d")
last_month = (first - datetime.timedelta(days=1)).strftime("%Y%m")
first_last_month = (first - datetime.timedelta(days=35)).strftime("%Y%m%d")
print first_last_month
last = (first - datetime.timedelta(days=1)).strftime("%Y%m%d")
first = str(first.strftime("%Y%m%d"))
last = str(last)
#storeIUDs = "zdSXUuPY0JM;Prqml6Ozl51;bLzjuZDFT3I;zAz2SAaxjEI;mS1oRZYQ41i;XSSIZp6iqWZ;Zk3XqT3Bntq"
storeIUDs = getStoreIDs()

response_log_str = 'Monthly_Opening_DataSet_response_' + str(today)
response_log = open('C:\scripts\dhis2\ETL\logs\\'+response_log_str,'w') #open the response log
Monthly_DataSet_file = 'C:\scripts\dhis2\ETL\logs\Monthly_Opening_DataSet_'+str(today)+'.json' #create a log for the JSON payload
f = open(Monthly_DataSet_file,'w') #open the log for writing

# Get todays stock on hand aggreated by item code for this month - openning stock balance
url = "https://dhis2.co/msupply/api/analytics/events/aggregate/YScaWpjOOnI.json?stage=VgrIh62C2gl&dimension=ou:"+ storeIUDs +"&dimension=pe:"+first_last_month+"&dimension=tnY9kPc9jTh&value=GMxEEg91qGO&aggregationType=SUM" #define the DHIS2 API url for the events, org units will be identified via mSupply codes
headers = {'Content-type': 'application/json'} #define the payload as JSON
r = requests.get(url, headers=headers, auth=HTTPBasicAuth('kgbolger', 'Ireland15?'), verify = False) #POST with basic authentication

json_data = json.loads(r.content)
ids = []
dataValues = {}; 
dataValues["dataValues"] = [] 

for node in json_data["rows"]:
	value = {};
	dataValues["dataValues"].append(value);

	value["dataElement"] = getOpeningBalUID(node[0])
	value["period"] = last_month
	value["orgUnit"] = node[1]
	if node[3].endswith('.0'):
		value["value"] = node[3][:-2]
	elif node[3].endswith('E4'):
		node[3] = float(node[3][:-2])
		node[3] = str(node[3]*10000)
	elif node[3].endswith('E5'):
		node[3] = float(node[3][:-2])
		node[3] = str(node[3]*100000)
		value["value"] = node[3][:-2]
	elif node[3].endswith('E6'):
		node[3] = float(node[3][:-2])
		node[3] = str(node[3]*1000000)
		value["value"] = node[3][:-2]
	elif node[3].endswith('E7'):
		node[3] = float(node[3][:-2])
		node[3] = str(node[3]*10000000)
		value["value"] = node[3][:-2]
	elif node[3].endswith('E8'):
		node[3] = float(node[3][:-2])
		node[3] = str(node[3]*100000000)
		value["value"] = node[3][:-2]

j = json.dumps(dataValues) #dump to create the JSON from the dictonary
print >> f, j #write the JSON payload to the log

#This section handles the JSON POST
url = "https://dhis2.co/msupply/api/dataValueSets?importStrategy=CREATE_AND_UPDATE" #define the DHIS2 API url for the events, org units will be identified via mSupply codes
headers = {'Content-type': 'application/json'} #define the payload as JSON
r = requests.post(url, data=j, headers=headers, auth=HTTPBasicAuth('kgbolger', 'Ireland15?'), verify = False) #POST with basic authentication

json_data = json.loads(r.content) #read the JSON response
print json_data
print >> response_log, json_data #write the full response to the log
http_status = json_data['status'] #find the http status code of the POST operation
ignored_count = json_data['importCount']['ignored'] #find if any records were ignored
if http_status == "SUCCESS" and ignored_count == 0: #if the http status is 200 'OK' and nothing was ignored;
    print 'Success!!' #tell me it worked
    print >> response_log, 'Success!!'

