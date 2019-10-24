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
from Master_functions import getAMC, setType, monthdelta, setDonor, setProgramByDonor, setProgramByItem

#import the required config file values
config = ConfigParser.RawConfigParser()   
configFilePath = 'C:\scripts\dhis2\ETL\msupply_dhis2_ETL_GLOBAL.cfg'
config.read(configFilePath)
stores_exclude = config.get('store_config','global_exclusion_stores')
items = config.get('item_config','global_item_list')
last_update = config.get('validation_config','Trans_lastupdate')
today = datetime.date.today() #set todays date
response_log_str = 'Validation_transaction_response_' + str(today)
response_log = open('C:\scripts\dhis2\ETL\logs\\'+response_log_str,'w') #open the response log
print >> response_log, last_update  #wrote todays date to the response log
start_time = str("Started: "+str(datetime.datetime.now().time()))
print >> response_log, start_time

mSupply_LIVE_connection = pyodbc.connect("DSN=mSupply local 32 bit", autocommit=True)
query_cursor = mSupply_LIVE_connection.cursor()

###############################################################################################################################################################
###############################################################################################################################################################
# This section generates a list of new transaction line ID's that need to be imported.
# First it gets all existing event ID's (transaction lines) from the from the transaction event program  - this is a list of mSupply transactions already
# in DHIS2.
# Next the script generates a list of all transaction ID's in mSupply that match the DHIS2 event criteria (items, status, stores etc).
# Lists are compared and a new list of ID's that are in mSupply but are NOT in DHIS2 is generated
###############################################################################################################################################################
print "Getting existing transaction IDs from DHIS2 via JSON"
print >> response_log, "Getting existing transaction IDs from DHIS2 via JSON "+str(datetime.datetime.now().time())
url = "https://dhis2.co/msupply/api/events.json?program=EgXN4cJ004K&skipPaging=TRUE"
headers = {'Content-type': 'application/json'} #define the payload as JSON
r = requests.get(url, headers=headers, auth=HTTPBasicAuth('kgbolger', 'Ireland15?'), verify = False) #POST with basic authentication
json_data = json.loads(r.content)
ids = []
for node in json_data["events"]:
    for x in range(0,len(node["dataValues"])):
        if node["dataValues"][x]["dataElement"] == "Qz8YmNgwqmO":
            ids.append(node["dataValues"][x]["value"])
print >> response_log, "Completed!!  Found "+str(len(ids))+" ID's in DHIS2 @ "+str(datetime.datetime.now().time())  
#ids = ['t1','t4','t2','t3','t1']
print [item for item, count in collections.Counter(ids).items() if count > 1]



# #builing the query string, item and store variable valuse are taken from config file
# trans_ID_query_str = str("select `trans_line`.`ID`"+
# "from `trans_line`" +
# "inner join `transact`" +
# "on `trans_line`.`transaction_ID` = `transact`.`ID`" +
# "inner join `item`" +
# "on `trans_line`.`item_ID` = `item`.`ID`" +
# "inner join `store`" +
# "on `transact`.`store_id` = `store`.`ID`" +
# "inner join `name`" +
# "on `transact`.`name_id` = `name`.`id`" +
# "WHERE not `store`.`code` like '%DAM%' and not `store`.`code` like '%PAM%' and not `store`.`code` in "+ stores_exclude +
# " and `trans_line`.`item_ID` in " + items +" and `transact`.`status` in ('cn','fn') and not `transact`.`type` = 'sr' and `transact`.`confirm_date` BETWEEN '2015-01-02' AND '2050-12-31' " +
# "order by `transact`.`confirm_date` DESC")
# #print the full query string to the respponse log for troubleshooting
# print >> response_log, "Query for transaction line ID's in mSupply:"
# print >> response_log, trans_ID_query_str

# transaction_lines = query_cursor.execute(trans_ID_query_str) #execute the query
# print >> response_log, "SQL query for transaction lines Complete!!  "+str(datetime.datetime.now().time())  
# id_list = "("
# ID_data = transaction_lines.fetchall() #assign all results to line_data
# if not ID_data:#check for results
#     print >> response_log,"no ID records returned on query:"+str(datetime.datetime.now().time()) #if none, tell the log
# elif ID_data:
#     print "Comparing lists"
#     print >> response_log, "Comparing lists @ "+str(datetime.datetime.now().time())
#     for ID in ID_data:
#         if ID.ID not in ids:
#             id_list+=str("'"+ID.ID+"',")
# id_list += "'')" # id_list is the list of ID's that need to be imported

# print >> response_log, "Completed generating list of new transaction lines to import!! @ "+str(datetime.datetime.now().time())
# print >> response_log, "Found "+str(len(id_list))+" new transactions ID's to import"
# ###############################################################################################################################################################
# ###############################################################################################################################################################




# ###############################################################################################################################################################
# ###############################################################################################################################################################
# # This section queries the relevent data of the new transactions to be imported and generates the JSON payload
# ###############################################################################################################################################################
# trans_query_str = str("select `store`.`code` as 'orgUnit', `name`.`code` as 'recStoreCode', `name`.`name` as 'recStoreName', `item`.`code` as 'itmCode', `transact`.`confirm_date`, `transact`.`type` as 'ITyp', `trans_line`.`type` as 'LTyp', `trans_line`.`quantity`, `trans_line`.`batch`, `t1`.`code` as 'DONOR', `trans_line`.`pack_size`, `trans_line`.`expiry_date`, `trans_line`.`ID`, `trans_line`.`user_1` as TYPE, `trans_line`.`user_2` as PROGRAM "+
# "from `trans_line` " +
# "inner join `transact` " +
# "on `trans_line`.`transaction_ID` = `transact`.`ID` " +
# "inner join `item` " +
# "on `trans_line`.`item_ID` = `item`.`ID` " +
# "inner join `store` " +
# "on `transact`.`store_id` = `store`.`ID` " +
# "inner join `name` " +
# "on `transact`.`name_id` = `name`.`id` " +
# "left join `name` `t1` " +
# "on `trans_line`.`donor_id` = `t1`.`id` " +
# "where `trans_line`.`ID` in "+ id_list +
# " order by `transact`.`confirm_date` DESC ")
# #print the full query string to the respponse log for troubleshooting
# print >> response_log, "Query string for new transactions, full data lines"
# print >> response_log, trans_query_str
# print "Getting list of confirmed transactions not in DHIS2"
# print >> response_log, "Executing query for new mSupply transaction data @ "+str(datetime.datetime.now().time())
# transaction_lines = query_cursor.execute(trans_query_str) #execute the query
# line_data = transaction_lines.fetchall() #assign all results to line_data
# print "Complete!"
# print >> response_log, "Query completed!! @ "+str(datetime.datetime.now().time())
# start_time = str("Time_check: "+str(datetime.datetime.now().time()))
# print >> response_log, start_time
# if not line_data:#check for results
#     print >> response_log,"no records returned on query:" #if none, tell the log
#     config.set('validation_config', 'Trans_lastupdate', today) #update the config file with the date it last worked (required for transactions as appose to current SoH)
#     with open('msupply_dhis2_etl_mchc.cfg','wb') as configfile: #open the config file for writing
#         config.write(configfile) #write the updates
#     print 'No records to write to mSupply DHIS2 Transaction Event Program - Terminating script as planned!!' #tell me it worked
# elif line_data: #if there are results
#     for row in line_data: #each row of the result set (which is a single transaction or line on an mSupply invoice)
#         #print row.ID
#         row.orgUnit = row.orgUnit.rstrip()
#         row.recStoreCode = row.recStoreCode.rstrip()
#         row.TYPE = setType(str(row.itmCode)) #setType function assigns a TYPE to the item (Depo is 'injection', Microlut is 'Oral' etc)
#         row.QUANTITY = row.QUANTITY*row.PACK_SIZE #to count in single units multiple the number of packs by the pack size
#         if row.ITyp == 'sc' and row.LTyp == 'stock_in': #check if the sc stock direction bug was observed (occurs when IA reduce is generated out of a ST)
#             row.LTyp = 'stock_out' #if the condition is observed then flip the line type direction
#         if row.BATCH == '': #check to see if the batch is empty
#             row.BATCH = 'NA' #if it is then set it to 'NA'
#         if row.DONOR == '': #check to see if the batch is empty
#             row.DONOR = 'NA' #if it is then set it to 'NA'            
#         if row.EXPIRY_DATE is None: #check to see if the expiry is empty
#             row.EXPIRY_DATE = datetime.date(1900,01,01) #if so, set it to 1900-12-31
#         if row.recStoreCode == 'invad' and row.ITyp == 'si': #check to see if the transaction is an IA to add items
#             row.ITyp = 'iaa' #if so, set the transaction type to iaa (code will result in Inventory Adjustment Add in event record)
#         if row.recStoreCode == 'invad' and row.ITyp == 'sc': #check to see if the transaction is an IA to reduce items
#             row.ITyp = 'iar' #if so, set the transaction type to iar (code will result in Inventory Adjustment Reduce in event record)            
#         if row.itmCode in ("CONA,ConGr,CONR,CONS,CON,CONSM") and row.DONOR != '':
#             row.PROGRAM = setProgramByDonor(row.itmCode)
#         if row.PROGRAM == '':
#             row.PROGRAM = setProgramByItem(row.itmCode)


#     start_time = str("Time_check: "+str(datetime.datetime.now().time()))
#     print >> response_log, start_time    
#     #following section builds the JSON payload based on the transformed result set
#     def dataValue(de, val): #dataValue function to create dictonary entries
#         return {"dataElement": de, "value": val}; #take the passed values, return dictonary entries

#     events = {}; #create a list of events
#     events["events"] = [] #in the list of events create an array called 'events'
#     print >> response_log, "Building JSON"
#     for row in line_data: #for each line in the transformed resultset
#         event = {}; #create a list called event
#         events["events"].append(event); #in the events array, add this list 'event'


#         event["orgUnit"] = row.orgUnit #define the orgUnit (store ID)
#         event["eventDate"] = str(row.CONFIRM_DATE) #define the incident date as invoice confirmation date
#         event["enrollmentStatus"] = "ACTIVE" #default value needed for JSON payload format
#         event["status"] = "ACTIVE" #default value needed for JSON payload format
#         event["program"] = "EgXN4cJ004K" #define the program the payload is intended for (mSupply Transaction Events)
#         event["dueDate"] = str(row.CONFIRM_DATE) #default value needed for JSON payload format

#         event["dataValues"] = []; #in the event, create an array to handle the data values

#         event["dataValues"].append(dataValue("tnY9kPc9jTh", str(row.itmCode))) #add the item name (using its code)
#         event["dataValues"].append(dataValue("RLQvf7IEd5a", str(row.itmCode))) #add the item code (we may only need one of these in the furture)
#         event["dataValues"].append(dataValue("L5IBaY2q055", str(row.CONFIRM_DATE))) #write the invoice confirmation date
#         event["dataValues"].append(dataValue("urAwnJWpKYj", str(row.ITyp))) #write the invoice type
#         event["dataValues"].append(dataValue("y5gmjQRi54V", str(int(row.QUANTITY)))) #write the quantity of individual items
#         event["dataValues"].append(dataValue("u8HoT9vKLyv", str(row.BATCH))) #wrote the batch info
#         event["dataValues"].append(dataValue("tlyKzBRKjSE", str(row.EXPIRY_DATE))) #write the batch expiry date
#         event["dataValues"].append(dataValue("yLGsiqTl6NM", str(row.DONOR))) #default donor until we figure out the query
#         event["dataValues"].append(dataValue("DxNWgkTcRAf", str(row.PROGRAM))) #default program tag
#         event["dataValues"].append(dataValue("Qz8YmNgwqmO", str(row.ID))) #write the individual transaction ID
#         event["dataValues"].append(dataValue("NJ878LXwoM9", str(row.recStoreCode))) #write the OrgUnit name (via option set) that was on the invoice
#         event["dataValues"].append(dataValue("mE5uvzGSlAc", str(row.TYPE))) #write the type of item (see def above)

#     j = json.dumps(events) #dump to create the JSON from the dictonary
#     events_file = 'C:\scripts\dhis2\ETL\logs\\Weekly_transaction_events_'+str(today)+'.json' #create a log for the JSON payload
#     f = open(events_file,'w') #open the log for writing
#     print >> f, j #write the JSON payload to the log
    
#     start_time = str("Sending JSON - time_check: "+str(datetime.datetime.now().time()))
#     print start_time
#     print >> response_log, start_time     
    
#     #This section handles the JSON POST
#     # print "Writing new transactions to DHIS2 via JSON"
#     url = "https://dhis2.co/msupply/api/events?orgUnitIdScheme=CODE&dryRun=FALSE" #define the DHIS2 API url for the events, org units will be identified via mSupply codes
#     headers = {'Content-type': 'application/json'} #define the payload as JSON
#     r = requests.post(url, data=j, headers=headers, auth=HTTPBasicAuth('kgbolger', 'Ireland15?'), verify=False) #POST with basic authentication

#     json_data = json.loads(r.content) #read the JSON response
#     http_status = json_data['httpStatusCode'] #find the http status code of the POST operation
#     ignored_count = json_data['response']['ignored'] #find if any records were ignored
#     print >> response_log, json_data #write the full response to the log
#     print >> response_log, start_time
#     print >> response_log, str("Finished: "+str(datetime.datetime.now().time()))
#     if http_status == 200 and ignored_count == 0: #if the http status is 200 'OK' and nothing was ignored;
#         print 'Success!!' #tell me it worked
#         print >> response_log, "Sucess!!"
#         config.set('validation_config', 'Trans_lastupdate', today) #update the config file with the date it last worked (required for transactions as appose to current SoH)
#     with open('C:\scripts\dhis2\ETL\msupply_dhis2_ETL_GLOBAL.cfg','wb') as configfile: #open the config file for writing
#         config.write(configfile) #write the updates
#     #COMLETE