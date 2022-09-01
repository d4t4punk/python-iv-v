# Document to Salesforce - multithreaded
# Author: Scott Newby (Steampunk)
# Date: 2022.08.08
# Description:
#   Move documents from a source system, such as db, or CSV to salesforce via SFDX/Cli interface
#   WRITTEN as a starting point to complete - TODO - create config, locate it
# Imports
from asyncio import Task
import codecs
from logging import exception
import re
import sys
import time
from datetime import datetime
import os
from urllib.parse import uses_relative
import requests
import json
# import psycopg2
# import ibm_db
import subprocess
import base64
from threading import Thread
import yaml
import csv

# globals
sstaticOwnerId = ''

# SF Endpoints
urlSFContentVersion = '../v54.0/sobjects/ContentVersion'
urlSFContentDocLink = '../v54.0/sobjects/ContentDocumentLink'

# setup lower environments or put into config...

def mainStart():
    print(time.ctime(), 'main start')
    with open('config location') as cfg:
        config = yaml.safe_load(cfg)
    
    # define source connection - SQL/CSV/something else to loop over

    # postgres connection if needed

    # SQL dir if needed
    global sqldir
    sqldir = config['someid']['someloc']
    global usr
    usr = config['some-sec-id']['usernm']
    global soqldir
    soqldir = config['someid']['someloc']

    #resetTracking()
    getSalesfroceIds()
    getSFToken()

    try:
        # need to determine threading logic per your source - sql - use query to divide up
        # csv - maybe just individual files
        csv1 = 'some loc'
        csv2 = 'some loc'
        csv3 = 'some loc'
        # start each thread
        t1 = Thread(target=loopBinaryRecords, args=(csv1))
        t2 = Thread(target=loopBinaryRecords, args=(csv2))
        t3 = Thread(target=loopBinaryRecords, args=(csv3))

        #wait for them to complete
        t1.start()
        t2.start()
        t3.start()

        t1.join()
        t2.join()
        t3.join()
    except (Exception) as error:
        print(time.ctime(), error)
    finally:
        print(time.ctime(), 'complete main start')

def getSalesfroceIds():
    print(time.ctime(), 'get sf ids')

    try:
        # truncate any tracking db table or file?
        # call function
        # get and read the soql from the soql dir?
        # alternatively - just do static query
        soql_file = open(soqldir + 'name-of-your-soql-file.soql')
        soql = soql_file.read()
        # execute against your instance to get data back in json
        retdata = subprocess.check_output(
            f'sfdx force:data:soql:query -q "' + soql + '" -u ' + usr + ' --json', shell=True, universal_newlines=True
        )
        print(retdata)
        # insert into some other db or something...
        # execInsSys('select schema.function_ins_here( %s )',retdata)
    except (Exception) as error:
        print(time.ctime(), error)
    finally:
        print(time.ctime(), 'complete get sf ids')

def loopBinaryRecords():
    print(time.ctime(), 'start loop binary records')
    try:
        #loop over some data set - recordset - csv data?
    
        filename = 'your-filename-here.csv'

        with open(filename, 'r') as csvfile:
            datareader = csv.reader(csvfile)
            for row in datareader:
                
                #title is filename
                sTitle = row['filename']
                sFirstPubLocId = row['id']
                sOwnerID = sstaticOwnerId
                sPathOnCli = sTitle
                bVersionData = base64.b64encode(row['bindarydata']).decode('utf-8')
                # Setup the payload
                sjdata = '{"Title":"' + sTitle + '","PathOnClient":"' + sPathOnCli + '","FirstPublishLocationId":"' + sFirstPubLocId + '","OwnerID":"' + sOwnerID + '","VersionData":"' + bVersionData + '"}'
                # post to sf
                sendFileToSF(sjdata)

    except (Exception) as error:
        print(time.ctime(), error)
    finally:
        print(time.ctime(), 'complete loop binary records')

def write_files(data,path,filename):
    print(time.ctime(), 'start write file')

    file, extension = os.path.splitext(filename)
    counter = 1
    fullfileloc = path + filename
    while os.path.exists(fullfileloc):
        filename = file+ " (" + str(counter) + ")" + extension
        fullfileloc = path + filename
        counter += 1

    filename = path + filename
    with open(filename, 'wb') as f:
        f.write(data)

def sendFileToSF(jdata, fn, id):
    print(time.ctime(), 'start send file to salesforce')
    try:
        # set the headers
        sf_headers = {'Authorization': 'Bearer ' + sfaccTkn, 'Content-Type':'application/json'}
        # send the request / get resp
        response = requests.post(urlSFContentVersion, data=jdata.encode('utf-8'), headers=sf_headers)
        jobj = response.json()
        print(time.ctime(), jobj['id', 'id return for file', fn, 'on id', id ])
        #track on separate list if needed
        #updateFileMigrationSFId(str(jobj['id']),fn,id)
    except (Exception) as error:
        print(time.ctime(), error)
    finally:
        print(time.ctime(),'complete send file to sf')

def setSFToken(strSFToken):
    global sfaccTkn
    sfaccTkn = strSFToken

def getSFToken():
    print(time.ctime(), 'start get sf token')
    try:
        retdata = subprocess.check_output(
            f'sfdx force:org:display -u ' + usr + ' --json', shell=True, universal_newlines=True
        )
    
        jdata = json.loads(retdata)
        tknJson = jdata['result']['accessToken']

        setSFToken(tknJson)
    except (Exception) as error:
        print(time.ctime(), error)
    finally:
        print(time.ctime(), 'complete with sf token')


mainStart()    

