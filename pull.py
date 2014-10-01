import pymongo
import sys, getopt
from pymongo import MongoClient
import time

"""
This program pulls data from a data set and deletes it. The idea is to simulate what the event builder will eventually look like from the DAQ reader side. It should run about one second behind real time and pull data in one second chunks. The data will actually be transferred to the machine on which this applet runs (so probably don't run it on the DB machine). Docs will be cleared from the database after they are transferred.
"""

def main(argv):
    dbAddr = 'xedaq00'  
    dbName = 'data'
    collName='test'
    print("Hello")
    # Parse command line arguments
    try:  
        opts, args = getopt.getopt(sys.argv[1:],"hs:d:c:",["server=","dbname=","collname="])  
    except getopt.GetoptError as err:  
        print(err)  
        print("sysmon.py -d <dbname> -s <server address> -c <collection name>")  
        sys.exit(2)  
    for opt, arg in opts:  
        if opt=='-h':  
            print("sysmon.py -d <dbname> -s <server address> -c <collection name>")  
            sys.exit(0)  
        elif opt in ("-s", "--server"):  
            dbAddr=arg  
        elif opt in ("-d", "--dbname"):  
            dbName=arg  
        elif opt in ("-c", "--collname"):
            collName=arg
    print("Server: "+dbAddr)  
    print("dbName: "+ dbName)  
    print("collName: "+ collName)
    # Connect to mongodb
    try:
        mongoClient = MongoClient(dbAddr)
    except RuntimeError:
        print("Error connecting to mongodb server")
        return    
    print("Connected to server")
    database = mongoClient[dbName]
    collection = database[collName]
#    collection = db[collName]
    
    #get run control document for start time. If not there wait 1 minute
    currenttime = (-1000000000)
    waited = 0
    first = True
    while currenttime < 0:
        rcDoc = collection.find_one({"time": {"$gt":0}},sort=[("time", pymongo.ASCENDING)])
        if rcDoc == None:
            if first == True:
                print("Collection doesn't seem to exist. I'll wait a minute for you to start the run")
                first = False
            time.sleep(1)
            waited+=1;
            if waited == 60:
                print("I waited a minute and still no run there. Run me again when you decide to take data")
                exit()
        else:
            currenttime = rcDoc['time']
                
    # Always pull one second of data at a time and kill it (pull first here)
    # Query that writing is always at least one second ahead of what you're 
    # pulling. i.e. that the first second will be pulled once a doc 2 seconds
    # ahead is inserted

    print ("Starting with current time = " + str(currenttime))
    latesttime = currenttime
    currenttime=0
    endtime = (-1)
    while endtime == -1:
        latestDoc = collection.find_one(sort=[("time",pymongo.DESCENDING)])
        if latestDoc == None:
            rcDoc = collection.find_one({"endtime"})
            if rcDoc != None:
                endtime = rcDoc.endtime
                continue
            print("No more docs, waiting")
            time.sleep(1)
            continue
        latesttime = latestDoc['time']
        
        if latesttime-currenttime < 2E8:
            print("Latest: " + str(latesttime) + "current: " + str(currenttime))
            time.sleep(50/1000.) # sleep for 50 ms (don't kill mongo w queries)
            continue
        print("Found latest time " + str(latesttime) + " with currenttime " + str(currenttime))
        doclist = collection.find({"time": {"$lt": currenttime+1E8}})
        collection.remove( { "time": {"$lt": currenttime+1E8 } } )
        print ("Just removed " + str(doclist.count()) + "docs from " + str(currenttime/1E8) + "until " + str((currenttime/1E8)+1) + "seconds")
        currenttime = currenttime + 1E8
        latesttime=currenttime
    print("Done! Dropping DB now")
    collection.drop()
    print("Dropped!")

if __name__ == "__main__":
    main(sys.argv[1:])
