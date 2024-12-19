""" This script checks the mongodb connection with possible try except blocks using pymongo python library; 
fetching the container list using docker engine API in python.
"""

# pymongo library helps interact with the mongodb instance and containers (tables, collections, deeper level of introspection)
from pymongo import MongoClient

def check_mongodb_connection(db,collection):
    try:
        # connect to the mongdb with host and map the port
        client = MongoClient("mongodb://10.255.255.254:27017/")
        # client = MongoClient("mongodb://localhost:27017")

        # The ismaster command is cheap and does not require auth - 
        # if client.admin.command('ismaster'):
        #     print("MongoDB connection successful.")
        # else:
        #     raise Exception
        # access the database
        dbase = client[db]
        # check if the collection is present in the database
        if collection in dbase.list_collection_names():
            print(f"{collection} exists in the {db}")
            # check if collection has documents
            docs = dbase[collection]
            if docs.estimated_document_count()>0:
                print(f"{collection} contains documents")
            else:
                print(f"{collection} is empty")
        else:
            print(f"{collection} does not exist in {db}")
    except Exception as e:
        print(f"Error occured : {e}") 

check_mongodb_connection(db="xml_data_db", collection="data_collection")

# installing docker library in python enables us to interact with docker engine's API, manage containers and such
import docker

def db_in_docker(dock_cmd):
# creating client object
    client = docker.from_env()
    try:
        # fetching the container list
        container= client.containers.get(dock_cmd)
        # check for active status
        if container.status == "running":
            print(f"container : {dock_cmd} is up and running")
        else:
            return f"container : {dock_cmd} exists but is not running {container.status}"
    except docker.errors.NotFound:
        print("container  not found : ", dock_cmd)
    except Exception as e:
        print("Error in container listing : ", e)

db_in_docker(dock_cmd="mongodb-container")

