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
from Master_functions import getAMC, setType, monthdelta, get24AMC_JSON,getUserAMC_JSON, setDonor, setProgramByDonor, setProgramByItem, getTransactionIds

#import the required config file values
config = ConfigParser.RawConfigParser()   
configFilePath = 'C:\scripts\dhis2\ETL\msupply_dhis2_ETL_GLOBAL.cfg'
config.read('C:\scripts\dhis2\ETL\msupply_dhis2_ETL_GLOBAL.cfg')
stores_exclude = config.get('store_config','global_exclusion_stores')
items = config.get('item_config','global_item_list')
last_update = config.get('validation_config','SoH_LastUpdate')
today = datetime.date.today() #set todays date
this_month = today.strftime("%Y%m")
tomorrow = datetime.date.today() + datetime.timedelta(days=1)
event_delete = 'C:\scripts\dhis2\logs\Master_SoH_events_' + str(tomorrow- timedelta(days=45)) + '.json'
response_delete = 'logs\Master_SoH_response_' + str(tomorrow- timedelta(days=45))
if os.path.isfile(event_delete):
    os.remove(event_delete)
if os.path.isfile(response_delete):
    os.remove(response_delete) 
print last_update #print the date
response_log_str = 'Master_SoH_response_' + str(tomorrow)
response_log = open('C:\scripts\dhis2\ETL\logs\\'+response_log_str,'w+') #open the response log
print >> response_log, last_update  #wrote tomorrows date to the response log
start_time = str("Started: "+str(datetime.datetime.now().time()))
#builing the query string, item and store variable valuse are taken from config file
query_str = str("SELECT `store`.`code` as orgUnit,`item`.`code` as itmCode,`item_line`.`pack_size`,`item_line`.`stock_on_hand_tot` as QUANTITY,`item_line`.`batch`,`item_line`.`expiry_date`, `item_line`.`user_1` as TYPE, `item_line`.`user_2` as AMC_tf, `item_line`.`user_2` as AMC_user, `item_line`.`user_3` as mSoH_tf, `item_line`.`user_3` as mSoH_user, `item_line`.`user_4` as MtE, `item_line`.`total_cost` as RoE_tf, `item_line`.`total_cost` as RoE_user, `item_line`.`total_cost` as PROGRAM, `t1`.`code`  as DONOR "+
    "FROM `item_line` " +
    "INNER JOIN `store` " +
    "ON `item_line`.`store_id` = `store`.`id` " +
    "INNER JOIN `item` " +
    "ON `item_line`.`item_id`=`item`.`id` " +
    "LEFT JOIN `name` `t1` " +
    "ON `item_line`.`donor_id` = `t1`.`id` " + 
    "WHERE not `store`.`code` like '%DAM%' and not `store`.`code` like '%PAM%' and not `store`.`code` in "+ stores_exclude +
    #"WHERE  `store`.`code` = 'VT.XA.CWH.STR'" +
    " and `item_line`.`item_ID` IN " + items +
    "AND NOT `item_line`.`stock_on_hand_tot` = 0 " +
    "ORDER BY `store`.`name`")

#print the full query string to the respponse log for troubleshooting
print >> response_log, query_str

#########################################################################################

#create the ODBC connection handle
mSupply_LIVE_connection = pyodbc.connect("DSN=mSupply local 32 bit", autocommit=True)
query_cursor = mSupply_LIVE_connection.cursor()
query_cursor.execute(query_str) #execute the query

line_data = query_cursor.fetchall() #assign all results to line_data
if not line_data: #check for results
    print >> response_log,"no records returned on query:"+'\n' # if no results, tell the log and stop here
elif line_data: #otherwise, if there are results.....
    print >> response_log,"JSON response to posted payload:"+'\n' #note it in the log
    for row in line_data: #for each row in the results (1 row is one stock line item), do the following transforms;
    	row.PROGRAM = 'NA'
        row.TYPE = setType(row.itmCode) #setType function assigns a TYPE to the item (Depo is 'injection', Microlut is 'Oral' etc)
        #print str("Calling get24AMC_JSON with: "+row.itmCode+" "+this_month+" "+row.orgUnit.strip('\r'))
        print(row.orgUnit)
        row.AMC_tf = str(get24AMC_JSON(row.itmCode,this_month,row.orgUnit.strip('\r'))) #get24AMC_JSON is a function to return the 24 AMC for the item/store combination for this period - stored in Monthly Agg DataSet
        if row.AMC_tf == '0':
            #print "Called zero fix TF"
            row.AMC_tf = 1
        # print str("Returned: " + str(row.AMC_tf))
        # print str("Calling getUserAMC_JSON with: "+row.itmCode+" "+this_month+" "+row.orgUnit)
        row.AMC_user = getUserAMC_JSON(row.itmCode,this_month,row.orgUnit.strip('\r')) #getAMC_JSON is a function to return the user defined AMC for the item/store combination for this period - stored in Monthly Agg DataSet
        if row.AMC_user == '0':
            print "Called zero fix User"
            row.AMC_user = 1        
        # print str("Returned: " +str(row.AMC_user))
        # print row.QUANTITY              
        row.mSoH_tf = round(float(row.QUANTITY)/float(row.AMC_tf),1) #mSoh is set at the total SoH divided but the AMC for that item
        #print str(row.QUANTITY+ " / "+row.AMC_tf)
        #print row.mSoH_tf
        row.mSoH_user = round(float(row.QUANTITY)/float(row.AMC_user),1) #mSoh is set at the total SoH divided but the AMC for that item
        # print row.mSoH_user
        if row.DONOR == '':
            row.DONOR = 'NA'
        if row.itmCode in ("CONA,ConGr,CONR,CONS,CON,CONSM") and row.DONOR != '':
            row.PROGRAM = setProgramByItem(row.itmCode)
        if row.PROGRAM == 'NA':
            row.PROGRAM = setProgramByDonor(row.itmCode)
        if row.PROGRAM == 'NA':
            row.PROGRAM = setProgramByItem(row.itmCode)
        if row.BATCH == '': #if there is no batch into;
            row.BATCH = 'NA' #set the batch info to 'NA'
        if row.EXPIRY_DATE is None: #if there is no Expiry date set;
            row.EXPIRY_DATE = datetime.date(1900,12,31) #set the expiry date to 1900-12-31  
        row.MtE = monthdelta(today,row.EXPIRY_DATE) #monthdelta returns the number of months between today and the expiry date of the batch = MtE       
        row.RoE_tf = row.mSoH_tf - row.MtE #RoE - risk of expiry is the differnece between the Months of Stock on Hand & the number of Months to Expiry...
        row.RoE_user = row.mSoH_user - row.MtE #RoE - risk of expiry is the differnece between the Months of Stock on Hand & the number of Months to Expiry...
        #a positive RoE number indicates the batch won't be used in time, the value is the number of months worth of stock that is at risk of expiring

    #following section build the JSON payload based on the transformed result set
    def dataValue(de, val): #dataValue function to create dictonary entries
        return {"dataElement": de, "value": val}; #take the passed values, return dictonary entries

    events = {}; #create a list of events
    events["events"] = [] #in the list of events create an array called 'events'

    for row in line_data: #for each line in the transformed resultset
        event = {}; #create a list called event
        events["events"].append(event); #in the events array, add this list 'event'

        event["orgUnit"] = row.orgUnit.strip('\r') #define the orgUnit (store ID)
        event["eventDate"] = str(tomorrow) #define the incident date as tomorrow
        event["enrollmentStatus"] = "ACTIVE" #default value needed for JSON payload format
        event["status"] = "ACTIVE" #default value needed for JSON payload format
        event["program"] = "YScaWpjOOnI" #define the program the payload is intended for
        event["dueDate"] = str(tomorrow) ##default value needed for JSON payload format

        event["dataValues"] = []; #in the event, create an array to handle the data values

        event["dataValues"].append(dataValue("tnY9kPc9jTh", str(row.itmCode))) #add the item name (using its code)
        event["dataValues"].append(dataValue("RLQvf7IEd5a", str(row.itmCode))) #add the item code (we may only need one of these in the furture)
        event["dataValues"].append(dataValue("GMxEEg91qGO", str(int(row.QUANTITY)))) #write out quantity 'current stock on hand'
        event["dataValues"].append(dataValue("u8HoT9vKLyv", str(row.BATCH))) #wrote the batch info
        event["dataValues"].append(dataValue("tlyKzBRKjSE", str(row.EXPIRY_DATE))) #write the expiry info
        event["dataValues"].append(dataValue("yLGsiqTl6NM", str(row.DONOR))) #default donor until we figure out the query
        event["dataValues"].append(dataValue("DxNWgkTcRAf", str(row.PROGRAM))) #default program tag
        event["dataValues"].append(dataValue("XDtOsbxXNZq", str(row.mSoH_tf))) #write the Months of Stock on Hand
        event["dataValues"].append(dataValue("mqiAj7iW8zX", str(row.mSoH_user))) #write the Months of Stock on Hand
        event["dataValues"].append(dataValue("KnpWCG1Hjg6", str(row.MtE))) #wrote the Months to expiry of the batch line
        event["dataValues"].append(dataValue("n8Ue4fK2C5k", str(row.AMC_tf))) #write the AMC value used in this calculation
        event["dataValues"].append(dataValue("SYozJJTqV2O", str(row.AMC_user))) #write the AMC value used in this calculation
        event["dataValues"].append(dataValue("mE5uvzGSlAc", str(row.TYPE))) #write the type of item (see def above)
        event["dataValues"].append(dataValue("NudFhZVTKg0", str(row.RoE_tf))) #write the Risk of expiry number
        event["dataValues"].append(dataValue("ycFiZlgR6Yb", str(row.RoE_user))) #write the Risk of expiry number

    j = json.dumps(events) #dump to create the JSON from the dictonary
    events_file = 'C:\scripts\dhis2\ETL\logs\Master_SoH_events_'+str(tomorrow)+'.json' #create a log for the JSON payload
    f = open(events_file,'w+') #open the log for writing
    print >> f, j #write the JSON payload to the log

    #This section handles the JSON POST
    url = "https://dhis2.co/msupply/api/events?orgUnitIdScheme=CODE" #define the DHIS2 API url for the events, org units will be identified via mSupply codes
    headers = {'Content-type': 'application/json'} #define the payload as JSON
    r = requests.post(url, data=j, headers=headers, auth=HTTPBasicAuth('kgbolger', 'Ireland15?'), verify=False) #POST with basic authentication
    
    print >> response_log, start_time
    print >> response_log, str("Finished: "+str(datetime.datetime.now().time()))
    # print start_time
    # print str("Finished: "+str(datetime.datetime.now().time()))

    json_data = json.loads(r.content) #read the JSON response
    http_status = json_data['httpStatusCode'] #find the http status code of the POST operation
    ignored_count = json_data['response']['ignored'] #find if any records were ignored
    print >> response_log, json_data #write the full response to the log
    if http_status == 200 and ignored_count == 0: #if the http status is 200 'OK' and nothing was ignored;
        print 'Success!!' #tell me it worked
        config.set('validation_config', 'SoH_LastUpdate', tomorrow) #update the config file with the date it last worked (required for transactions as appose to current SoH)
with open('msupply_dhis2_etl_mchc.cfg','wb') as configfile: #open the config file for writing
    config.write(configfile) #write the updates
# COMPLETE


# url = "https://dhis2.co/msupply/api/events?orgUnitIdScheme=CODE"
# headers = {'Content-type': 'application/json'}
# r = requests.post(url, data=j, headers=headers, auth=HTTPBasicAuth('user', 'pass'))