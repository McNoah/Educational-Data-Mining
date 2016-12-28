import os
import pandas as pd
import numpy as np
import logging
from utils import Paths
from decimal import Decimal
import time
import collections
import encodings
import re
from os import path
import mysql.connector
from datetime import datetime

CHUNKSIZE = 100000 # processing 100,000 rows at a time

#-----------preprocessing data------------
def preprocessing():
    print ('preprocessing raw data')
    start_time = time.time()

    if os.path.exists(Paths.data):
        statinfo = os.stat(Paths.data)
        if statinfo.st_size > 0:
            readRawFolder()

def readRawFolder():
    if not os.path.exists(Paths.data):
        os.makedirs(Paths.data)
        os.makedirs(Paths.result)
        print('Input data not found, please put Web log data to folder named data :)')
        return

    print("Connecting to MySql...")
    db = mysql.connector.connect("localhost", user="root", password="12345", database="FUWebLog", use_unicode=True, charset="utf8")
    print("Connected to MySql - DB: FUWebLog")
    # prepare a cursor object using cursor() method
    cursor = db.cursor()

    #create table in MySQL if need
    createTableIfNeed(db, cursor)

    dirs = os.listdir(Paths.data)
    # This would print all the files and directories
    for fileName in dirs:
        if os.path.isdir(Paths.data + "/" + fileName):
            pathName = os.listdir(Paths.data + "/" + fileName)
            for fileSub in Paths.data:
                readRawFile(fileSub, db, cursor)
        else:
            readRawFile(fileName, db, cursor)

    print("Disconnect from MySql - DB: FUWebLog")
    db.close()

def readRawFile(fileName, db, cursor):
    filePath = Paths.data + "/" + fileName
    if not os.path.exists(filePath):
        print('File Not Found!!!')
        return

    if os.path.splitext(fileName)[1][1:].strip().lower() != "csv":
        return

    print("Reading: " + filePath)
    #Try to spit data to smaller parts and read
    reader = pd.read_table(filePath, chunksize=CHUNKSIZE, sep=",", encoding='utf-8')

    result = getItemsCount(db, cursor)
    print ("There are {0:d} rows of data".format((result)))

    print("Start import data to MySQL server...")
    for df in reader:
        # process each data frame
        result += process_frame(df, db, cursor)
        print("FUWebLog: Imported {0:d} records".format(result))

    print ("There are {0:d} rows of data".format((result)))

def process_frame(df, db, cursor):
    # process data frame
    # Write data to db
    importWebLog(df, db, cursor)
    return len(df)

#-----------MySQL------------
def isHaveTable(db, cursor):
    result = False
    sql = "SELECT * \
            FROM information_schema.tables \
            WHERE table_schema = 'FUWebLog' \
            AND table_name = 'WebProxyLog' \
            LIMIT 1;"
    try:
        # Execute the SQL command
        cursor.execute(sql)
        # Fetch all the rows in a list of lists.
        results = cursor.fetchall()
        result = len(results) > 0
    except:
        result = False
    return result

def getItemsCount(db, cursor):
    sql = "SELECT COUNT(*) \
            FROM FUWebLog.WebProxyLog;"
    try:
        # Execute the SQL command
        cursor.execute(sql)
        # Fetch all the rows in a list of lists.
        results = cursor.fetchall()
        return results[0][0]
    except:
        return 0

def createTableIfNeed(db, cursor):
    print("FUWebLog: Check table WebProxyLog...")
    if isHaveTable(db, cursor):
        print("FUWebLog: Already have table WebProxyLog...")
        return

    # Drop table if it already exist using execute() method.
    cursor.execute("DROP TABLE IF EXISTS WebProxyLog")
    # Create table as per requirement
    sql = """CREATE TABLE WebProxyLog (
             ClientIP CHAR(38) NOT NULL,
             ClientUserName NVARCHAR(514),
             ClientAgent VARCHAR(128),
             ClientAuthenticate INT,
             logTime DATETIME,
             service SMALLINT,
             servername NVARCHAR(32),
             referredserver VARCHAR(255),
             DestHost NVARCHAR(255),
             DestHostIP CHAR(38),
             DestHostPort INT,
             processingtime INT,
             bytesrecvd BIGINT,
             bytessent BIGINT,
             protocol VARCHAR(13),
         	 transport VARCHAR(8),
         	 operation VARCHAR(24),
         	 uri VARCHAR(2048),
         	 mimetype VARCHAR(32),
         	 objectsource SMALLINT,
         	 resultcode INT,
         	 CacheInfo INT,
         	 rule NVARCHAR(128),
         	 FilterInfo NVARCHAR(256),
         	 SrcNetwork NVARCHAR(128),
         	 DstNetwork NVARCHAR(128),
         	 ErrorInfo INT,
         	 Action SMALLINT,
         	 GmtLogTime VARCHAR(50),
         	 AuthenticationServer VARCHAR(255),
         	 ipsScanResult SMALLINT,
         	 ipsSignature NVARCHAR(128),
         	 ThreatName VARCHAR(255),
         	 MalwareInspectionAction SMALLINT,
         	 MalwareInspectionResult SMALLINT,
         	 UrlCategory INT,
         	 MalwareInspectionContentDeliveryMethod SMALLINT,
         	 UagArrayId VARCHAR(20),
         	 UagVersion INT,
         	 UagModuleId VARCHAR(20),
         	 UagId INT,
         	 UagSeverity VARCHAR(20),
         	 UagType VARCHAR(20),
         	 UagEventName VARCHAR(60),
         	 UagSessionId VARCHAR(50),
         	 UagTrunkName VARCHAR(128),
         	 UagServiceName VARCHAR(20),
         	 UagErrorCode INT,
         	 MalwareInspectionDuration INT,
         	 MalwareInspectionThreatLevel SMALLINT,
         	 InternalServiceInfo INT,
         	 ipsApplicationProtocol NVARCHAR(128),
         	 NATAddress CHAR(38),
         	 UrlCategorizationReason SMALLINT,
         	 SessionType SMALLINT,
         	 UrlDestHost VARCHAR(255),
         	 SrcPort INT,
             WebTitle TEXT(21844) CHARACTER SET utf8,
             WebDescription TEXT(21844) CHARACTER SET utf8,
             WebKeywords TEXT(21844) CHARACTER SET utf8,
             Category TEXT(21844) CHARACTER SET utf8)"""

    cursor.execute(sql)
    print("FUWebLog: Create table WebProxyLog succesfully!!!")


def IntToDottedIP(intip):
		octet = []
		for exp in [3,2,1,0]:
				k = 256 ** exp
				octet.append(str(intip // k))
				intip %=  k
		return '.'.join(octet)

def hex2dec(s):
	return int(s, 16)


def importWebLog(df, db, cursor):
    for index, row in df.iterrows():
        # Prepare SQL query to INSERT a record into the database.
        sql = "INSERT INTO WebProxyLog(ClientIP, \
                                    ClientUserName, \
                                    ClientAgent, \
                                    ClientAuthenticate, \
                                    logTime, \
                                    service, \
                                    servername, \
                                    referredserver, \
                                    DestHost, \
                                    DestHostIP, \
                                    DestHostPort, \
                                    processingtime, \
                                    bytesrecvd, \
                                    bytessent, \
                                    protocol, \
                                    transport, \
                                    operation, \
                                    uri, \
                                    mimetype, \
                                    objectsource, \
                                    resultcode, \
                                    CacheInfo, \
                                    rule, \
                                    FilterInfo, \
                                    SrcNetwork, \
                                    DstNetwork, \
                                    ErrorInfo, \
                                    Action, \
                                    GmtLogTime, \
                                    AuthenticationServer, \
                                    ipsScanResult, \
                                    ipsSignature, \
                                    ThreatName, \
                                    MalwareInspectionAction, \
                                    MalwareInspectionResult, \
                                    UrlCategory, \
                                    MalwareInspectionContentDeliveryMethod, \
                                    UagArrayId, \
                                    UagVersion, \
                                    UagModuleId, \
                                    UagId, \
                                    UagSeverity, \
                                    UagType, \
                                    UagEventName, \
                                    UagSessionId, \
                                    UagTrunkName, \
                                    UagServiceName, \
                                    UagErrorCode, \
                                    MalwareInspectionDuration, \
                                    MalwareInspectionThreatLevel, \
                                    InternalServiceInfo, \
                                    ipsApplicationProtocol, \
                                    NATAddress, \
                                    UrlCategorizationReason, \
                                    SessionType, \
                                    UrlDestHost, \
                                    SrcPort) \
               VALUES (%s, %s, %s,  %s ,  %s, \
                 %s ,  %s ,  %s ,  %s ,  %s , \
                 %s ,  %s ,  %s ,  %s ,  %s , \
                 %s ,  %s ,  %s ,  %s ,  %s , \
                 %s ,  %s ,  %s ,  %s ,  %s , \
                 %s ,  %s ,  %s ,  %s ,  %s , \
                 %s ,  %s ,  %s ,  %s ,  %s , \
                 %s ,  %s ,  %s ,  %s ,  %s , \
                 %s ,  %s ,  %s ,  %s ,  %s , \
                 %s ,  %s ,  %s ,  %s ,  %s , \
                 %s ,  %s ,  %s ,  %s ,  %s , \
                 %s ,  %s )"
        try:
           # Execute the SQL command
           # datetime.strptime(row[4], '%m/%d/%Y');
           cursor.execute(sql, (IntToDottedIP(hex2dec(row[0].split('-')[0])), row[1], row[2], row[3], datetime.strptime(row[4], '%m/%d/%Y %H:%M:%S').strftime('%Y/%m/%d %H:%M:%S') ,
                                row[5], row[6], row[7], row[8], IntToDottedIP(hex2dec(row[9].split('-')[0])),
                                row[10], row[11], row[12], row[13], row[14],
                                row[15], row[16], row[17], row[18], row[19],
                                row[20], row[21], row[22], row[23], row[24],
                                row[25], row[26], row[27], row[28], row[29],
                                row[30], row[31], row[32], row[33], row[34],
                                row[35], row[36], row[37], row[38], row[39],
                                row[40], row[41], row[42], row[43], row[44],
                                row[45], row[46], row[47], row[48], row[49],
                                row[50], row[51], row[52], row[53], row[54],
                                row[55], row[56]))
           # Commit your changes in the database
           db.commit()
        except Exception as error:
           # Rollback in case there is any error
           print(error)
           db.rollback()

#-----------Utils------------
def escapeString(value):
    return value.replace("'", "")

#-----------Web Crawler------------


#-----------Execute------------
def main():
    oper = -1
    while int(oper) != 0:
        print('**************************************')
        print('Choose one of the following: ')
        print('1 - Pre Processing and Extracting Raw Data')
        print('0 - Exit')
        print('**************************************')

        try:
            oper = int(input("Enter your options: "))
        except Exception as error:
            print("Please input a number!!!")

        if oper == 0:
            exit()
        elif oper == 1:
            preprocessing()

if __name__ == "__main__":
    main()
