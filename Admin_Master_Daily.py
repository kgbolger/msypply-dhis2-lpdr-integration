#!/usr/bin/env python

import os
import pyodbc
import json
import collections
import subprocess
import sys
import datetime 
from datetime import timedelta, date
import requests
import ConfigParser
from requests.auth import HTTPBasicAuth
from Master_functions import getSYNCname, getAMC, setType, monthdelta, get24AMC_JSON,getUserAMC_JSON, setDonor, setProgramByDonor, setProgramByItem, getTransactionIds



#import the required config file values
config = ConfigParser.RawConfigParser()   
configFilePath = 'C:\scripts\dhis2\ETL\msupply_dhis2_ETL_GLOBAL.cfg'  #define the config file
config.read('C:\scripts\dhis2\ETL\msupply_dhis2_ETL_GLOBAL.cfg')  #read the config file

today = datetime.date.today() #set todays date
tomorrow = today + timedelta(days=1)
three_months_ago = today - timedelta(days=90)
one_month_ago = today - timedelta(days=28)
this_month = today.strftime("%Y%m")
syncDict = {}

event_delete = 'C:\scripts\dhis2\ETL\logs\Admin_events_' + str(today- timedelta(days=45)) + '.json'
response_delete = 'C:\scripts\dhis2\ETL\logs\Admin_response_' + str(today- timedelta(days=45))
if os.path.isfile(event_delete):
    os.remove(event_delete)
if os.path.isfile(response_delete):
    os.remove(response_delete) 
response_log_str = 'Admin_response_' + str(tomorrow)
response_log = open('C:\scripts\dhis2\ETL\logs\\'+response_log_str,'w') #open the response log

#builing the query string, item and store variable valuse are taken from config file
# invoice_query_str = str("select store.code, invoice_num, entry_date, confirm_date, status, type, hold "+
# 	"from transact "+
# 	"join store " +
# 	"on transact.store_ID = store.ID " +
# 	"where store.code = 'SL.SL.PHO.MS.STR'  and entry_date > '2015-01-01'"+
# 	"order by store.code")

invoice_query_str = str("select store.code, invoice_num, entry_date, confirm_date, status, type, hold "+
	"from transact "+
	"join store " +
	"on transact.store_ID = store.ID " +
	"where not store.code like '%VTE.MH%' and not store.code like '%DAM%' and not store.code like '%PAM%' and not store.code in " +
	"('DRG','HIS','CDS.STR','VS.EXPG.STR','TRS2','VTE.MSH.SS','VT.CY.CMPE.STR','SM')  and entry_date > '2015-01-01'"+
	"order by store.code")

store_code_query_str = str("select code from store where not store.code like '%VTE.MH%' and not store.code like '%DAM%' and not store.code " +
	"like '%PAM%' and not store.code in ('DRG','HIS','CDS.STR','VS.EXPG.STR','TRS2','VTE.MSH.SS','VT.CY.CMPE.STR','SM')  order by store.code")

sync_date_query_str = str("select `entry_date`, `event` as orgUnit from `log` where `event_type` = 'sync_count' and `entry_date` > '"
	+ str(three_months_ago) + "' order by `event`,`entry_date` desc")
sync_store_query_str = str("select DISTINCT(`event`) as orgUnit from `log` where `event_type` = 'sync_count' order by `event`")

#following section build the JSON payload based on the transformed result set
def dataValue(de, val): #dataValue function to create dictonary entries
    return {"dataElement": de, "value": val}; #take the passed values, return dictonary entries

events = {}; #create a list of events
events["events"] = [] #in the list of events create an array called 'events'


#print the full query string to the respponse log for troubleshooting
print >> response_log, str("Invoice stats query: "+invoice_query_str +"\n")
print >> response_log, str("Store codes query: "+store_code_query_str +"\n")
print >> response_log, str("Sync date query: "+sync_date_query_str +"\n")
print >> response_log, str("Sync store codes query: "+sync_store_query_str +"\n")

#create the ODBC connection handle
mSupply_LIVE_connection = pyodbc.connect("DSN=mSupply local 32 bit", autocommit=True)
query_cursor = mSupply_LIVE_connection.cursor()
query_cursor.execute(invoice_query_str) #execute the query 

store_query_cursor = mSupply_LIVE_connection.cursor()
store_query_cursor.execute(store_code_query_str) #execute the query 

sync_query_cursor = mSupply_LIVE_connection.cursor()
sync_query_cursor.execute(sync_date_query_str)

#get a distinct list of sync codes and convert them to orgUnit codes
sync_code_query_cursor = mSupply_LIVE_connection.cursor()
sync_code_query_cursor.execute(sync_store_query_str)
sync_codes = sync_code_query_cursor.fetchall() 
sync_records = sync_query_cursor.fetchall()
for code in sync_codes:
	code_orgUnit = getSYNCname(str(code.orgUnit))
	sync_last_date = datetime.date(1900,1,1)
	sync_last_days = 0
	sync_90day_average = 0
	sync_times_past_month = 0
	sync_first_date = today
	average_days = []

#get the list of sync log records and generate stats
#loop through list based on list of 

	for record in sync_records:
		record_orgUnit = getSYNCname(str(record.orgUnit))
		if record_orgUnit == code_orgUnit and record.ENTRY_DATE>sync_last_date:
			sync_last_date = record.ENTRY_DATE
		if record_orgUnit == code_orgUnit:
			days = sync_first_date - record.ENTRY_DATE
			average_days.append(int(days.days))
			sync_first_date = record.ENTRY_DATE
		if record_orgUnit == code_orgUnit and record.ENTRY_DATE>one_month_ago:
			sync_times_past_month += 1
	sync_last_days = today - sync_last_date
	sync_last_days = int(sync_last_days.days)
	if len(average_days) == 0:
		average_days.append(0)
	sync_90day_average = sum(average_days)/len(average_days)
	if sync_90day_average == 0:
		sync_90day_average = 91
	if sync_last_days>91:
		sync_last_days = 91
	syncDict[code_orgUnit] = [sync_last_date.strftime("%Y-%m-%d"),sync_last_days,sync_90day_average,sync_times_past_month]

	# print str(code_orgUnit+": ") 
	# print syncDict[code_orgUnit][0]

#print "Completed the SYNC stats:"



#######################################################################################3
stores = store_query_cursor.fetchall() #assign all results to line_data
line_data = query_cursor.fetchall() #assign all results to line_data
for store in stores:
	

# Query should collect records for all stores and ordered by store code
# Loop through result sets for each store code
# at the end of a store code pass, append the JSON with that stores data


# counters
	CI_last_date = datetime.date(1900,1,1)
	CI_last_days = 0
	CI_WIP_last_14day = 0
	CI_WIP_older_14day = 0
	CI_CN_older_7day = 0
	CI_CN_older_90days = 0
	CI_FN_last_90days = 0
	SI_HOLD_older_14day = 0
	SI_new_NoHold_7days = 0
	SI_FN = 0

	if not line_data: #check for results
	    print >> response_log,"no records returned on query:"+'\n' # if no results, tell the log and stop here
	elif line_data: #otherwise, if there are results.....
	    print >> response_log,"JSON response to posted payload:"+'\n' #note it in the log
	    for row in line_data: #for each row in the results (1 row is one stock line item), do the following transforms;
	    	if row.CODE == store.CODE and row.TYPE == 'ci' and (row.ENTRY_DATE>CI_last_date):
	    		CI_last_date = row.ENTRY_DATE
	    	if row.CODE == store.CODE and row.TYPE == 'ci' and row.STATUS in ('nw','sg') and (today - row.ENTRY_DATE).days<14:
	    		CI_WIP_last_14day += 1
	    	if row.CODE == store.CODE and row.TYPE == 'ci' and row.STATUS in ('nw','sg') and (today - row.ENTRY_DATE).days>14:
	    		CI_WIP_older_14day += 1
	    	if row.CODE == store.CODE and row.TYPE == 'ci' and row.STATUS == 'cn' and (today - row.CONFIRM_DATE).days>7:
	    		CI_CN_older_7day += 1
	    	if row.CODE == store.CODE and row.TYPE == 'ci' and row.STATUS == 'cn' and (today - row.CONFIRM_DATE).days>90:
	    		CI_CN_older_90days += 1
	    	if row.CODE == store.CODE and row.TYPE == 'ci' and row.STATUS == 'fn' and int((today - row.CONFIRM_DATE).days)<90:
	    		CI_FN_last_90days += 1
	    	if row.CODE == store.CODE and row.TYPE == 'si' and row.STATUS == 'nw' and row.HOLD and (today - row.ENTRY_DATE).days>14:
	    		SI_HOLD_older_14day += 1
	    	if row.CODE == store.CODE and row.TYPE == 'si' and row.STATUS == 'nw' and not row.HOLD and (today - row.ENTRY_DATE).days>7:
	    		SI_new_NoHold_7days += 1
	    	# print row.CODE
	    	# print row.TYPE
	    	# print row.STATUS
	    	# print row.CONFIRM_DATE 
	    	if row.CODE == store.CODE and row.TYPE == 'si' and row.STATUS == 'fn' and int((today - row.ENTRY_DATE).days)<90:
	    		SI_FN += 1


	event = {}; #create a list called event
	events["events"].append(event); #in the events array, add this list 'event'

	event["orgUnit"] = store.CODE.strip('\r') #define the orgUnit (store ID)
	event["eventDate"] = str(tomorrow) #define the incident date as today
	event["enrollmentStatus"] = "ACTIVE" #default value needed for JSON payload format
	event["status"] = "ACTIVE" #default value needed for JSON payload format
	event["program"] = "A2VYVXg2nRs" #define the program the payload is intended for
	event["dueDate"] = str(tomorrow) ##default value needed for JSON payload format

	event["dataValues"] = []; #in the event, create an array to handle the data values
	CI_last_days = tomorrow - CI_last_date
	CI_last_days = int(CI_last_days.days)
	CI_CN_older_7day = CI_CN_older_7day - CI_CN_older_90days
	event["dataValues"].append(dataValue("xhrhD4Ikp5W", str(CI_last_date))) #add the item name (using its code)
	event["dataValues"].append(dataValue("smp9ZlXTe9m", str(CI_last_days))) #add the item name (using its code)
	event["dataValues"].append(dataValue("WdFZHE70SpY", str(CI_WIP_last_14day))) #add the item name (using its code)
	event["dataValues"].append(dataValue("XNdxVrtjf4T", str(CI_WIP_older_14day))) #add the item name (using its code)
	event["dataValues"].append(dataValue("qiPqDYqi9lO", str(CI_CN_older_7day))) #add the item name (using its code)
	event["dataValues"].append(dataValue("zlxYDSNphQe", str(CI_CN_older_90days))) #add the item name (using its code)
	event["dataValues"].append(dataValue("TpzYvptQvDY", str(CI_FN_last_90days))) #add the item name (using its code)
	event["dataValues"].append(dataValue("rT2VpYQv110", str(SI_HOLD_older_14day))) #add the item name (using its code)
	event["dataValues"].append(dataValue("OukXpKQAqLH", str(SI_new_NoHold_7days))) #add the item name (using its code)
	event["dataValues"].append(dataValue("tV94UrN6wzg", str(SI_FN))) #add the item name (using its code)

	#print str(store.CODE.strip('\r'))
	if str(store.CODE.strip('\r')) in syncDict:
		event["dataValues"].append(dataValue("k3RKFtunPxI", str(syncDict[str(store.CODE.strip('\r'))][0]))) #add the item name (using its code)
		event["dataValues"].append(dataValue("LW0cMaSTkik", str(syncDict[str(store.CODE.strip('\r'))][1]))) #add the item name (using its code)
		event["dataValues"].append(dataValue("HcGAueK6i9t", str(syncDict[str(store.CODE.strip('\r'))][2]))) #add the item name (using its code)
		event["dataValues"].append(dataValue("nWVeSsWwIfd", str(syncDict[str(store.CODE.strip('\r'))][3]))) #add the item name (using its code)

	# CI_last_days = int(today - CI_last_date)
	# print store.CODE
	# print "CI_last_date: "	+ str(CI_last_date)
	# print "CI_last_days: "	+ str(CI_last_days)
	# print "CI_WIP_last_14day: "	+ str(CI_WIP_last_14day)
	# print "CI_WIP_older_14day: "	+ str(CI_WIP_older_14day)
	# print "CI_CN_older_7day: "	+ str(CI_CN_older_7day)
	# print "CI_CN_older_90days: "	+ str(CI_CN_older_90days)
	# print "CI_FN_last_90days: "	+ str(CI_FN_last_90days)
	# print "SI_HOLD_older_14day: "	+ str(SI_HOLD_older_14day)
	# print "SI_new_NoHold_7days: "	+ str(SI_new_NoHold_7days)
	# print "SI_CN_Hold: "	+ str(SI_CN_Hold)
	# print "\n"
j = json.dumps(events) #dump to create the JSON from the dictonary
events_file = 'c:\scripts\dhis2\ETL\logs\Admin_events_'+str(tomorrow)+'.json' #create a log for the JSON payload
f = open(events_file,'w') #open the log for writing
print >> f, j #write the JSON payload to the log

#This section handles the JSON POST
url = "https://dhis2.co/msupply/api/events?orgUnitIdScheme=CODE" #define the DHIS2 API url for the events, org units will be identified via mSupply codes
headers = {'Content-type': 'application/json'} #define the payload as JSON
r = requests.post(url, data=j, headers=headers, auth=HTTPBasicAuth('kgbolger', 'Ireland15?'), verify=False) #POST with basic authentication

json_data = json.loads(r.content) #read the JSON response
http_status = json_data['httpStatusCode'] #find the http status code of the POST operation
ignored_count = json_data['response']['ignored'] #find if any records were ignored
print >> response_log, json_data #write the full response to the log
if http_status == 200 and ignored_count == 0: #if the http status is 200 'OK' and nothing was ignored;
    print 'Success!!' #tell me it worked