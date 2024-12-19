"""Before executing this script, ensure the docker engine is started.
From wsl `wsl -d docker-desktop`
or manually start the docker engine.
"""

import os
import time
# defining a function to run the docker commands using the module OS to interact with system(that facilitates CLI/bash like interaction).
def os_cmd(docker_cmd):
    # try except block for checking if the docker commands ran successfully
    try:
        # passing the command to pull mongodb image from docker as argument.
        chk=os.system(docker_cmd)
        # returns 0 if image pulled else error is thrown
        if chk != 0:
            raise Exception(f"error in docker : {docker_cmd}")
    # exception error on implemented logic
    except Exception as e:
        print(f"command failed : {e}")

# handling case where container already exists
def exist_cont(cmd2):
    try:
        # the below command lists all the stopped docker containers with the container name specified
        existing_container=os.system(f"docker ps -a --format '{{{{.Names}}}}' | grep -w mongodb-container")
        # if the search result yields container, then we proceed with the command to start the container
        if existing_container == 0:
            print("mongodb-container already exists")
            os_cmd(cmd2)
        # if the search result yields nothing, then we create one 
        else:
            print("creating new container")
            os_cmd("docker run --name mongodb-container -d -p 2017:2017 mongo")
        # the below exception is raised had there been errors while implementing the logic itself.
    except Exception as e:
        return f"Error is {e}"
# function call
print("pulling docker image for mongo and running the container from Docker")
os_cmd("docker pull mongo")
# time pause between functions
time.sleep(6)

exist_cont(cmd2="docker start mongodb-container")
print("mongodb container is running")
