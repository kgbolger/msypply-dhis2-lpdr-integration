#!/usr/bin/env python

import os
import pyodbc
import json
import urllib
import collections
import subprocess
import sys
import datetime
from datetime import timedelta, date
import requests
import ConfigParser
from requests.auth import HTTPBasicAuth
from functions import getAMC, setType, monthdelta,getSYNCname, dataValue

today = datetime.date.today() #set todays date

events_file = 'C:\scripts\dhis2\etl\logs\DHIS2_AnalyticsTablesUpdate_'+str(today)+'.json' #create a log for the JSON payload
f = open(events_file,'w') #open the log for writing
print >> f,str("Started: "+str(today)+" "+str(datetime.datetime.now().time()))
url = "https://dhis2.co/msupply/api/resourceTables/analytics?skipResourceTables=FALSE&skipEvents=FALSE&skipAggregate=FALSE&skipEnrollment=FALSE&lastYears=3"
headers = {'Content-type': 'application/json'} #define the payload as JSON
r = requests.put(url, headers=headers, auth=HTTPBasicAuth('kgbolger', 'Ireland15?'), verify = False) #POST with basic authentication
json_data = json.loads(r.content)

print >> f, json_data #write the JSON payload to the log
print >> f,str("Completed: "+str(today)+" "+str(datetime.datetime.now().time()))
