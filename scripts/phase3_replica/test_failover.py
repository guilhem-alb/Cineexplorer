from pymongo import MongoClient
from pymongo.errors import AutoReconnect
import time

def test_replication(pDB, s1DB, s2DB):
    try:
        pDB.create_collection("test")
        pDB["test"].insert_one({"name": "insert_test"})

        time.sleep(5) # wait for duplication

        if s1DB["test"].find_one({"name": "insert_test"}) and s2DB["test"].find_one({"name": "insert_test"}):
            print("insertion replicated succesfully")
        else:
            print("Failed replication on new insertion")
    finally:
        pDB.drop_collection("test")

def test_failover(pClient, s1Client, s2Client):
    try:
        pClient.admin.command("shutdown", 1)
    except AutoReconnect:
        print("Primary was shut down")
    t_init = time.time()

    while (not s1Client.is_primary) and (not s2Client.is_primary):
        # do nothing
        continue

    print("election time = ", time.time() - t_init)

    if s1Client.is_primary:
        pDB = s1Client["imdb"]
    else:
        pDB = s2Client["imdb"]

    if pDB["movies_complete"].find_one():
        print("Data is readable on new primary")



with MongoClient('mongodb://localhost:27017/') as pClient, MongoClient('mongodb://localhost:27018/') as s1Client, MongoClient('mongodb://localhost:27018/') as s2Client:
    pDB, s1DB, s2DB = pClient["imdb"], s1Client["imdb"], s2Client["imdb"]

    test_replication(pDB, s1DB, s2DB)
    test_failover(pClient, s1Client, s2Client)
