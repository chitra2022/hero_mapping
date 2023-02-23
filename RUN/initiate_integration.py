import logging as log
from flask import Flask
import os
import socket
from flask import request
from data_sync_mds_gcp import *
pwd = os.getcwd()
with open(F"{pwd}/config/config.json") as file:
    config=json.load(file)

log.basicConfig(level=log.DEBUG)
app = Flask(__name__)

@app.route("/")
def entrypoint():
    task_flow = request.args.get("task_flow")
    task_name = request.args.get("task_name")    
    task_flow_names =['inbound','outbound']
    logging.info("task_flow --> "+ str(task_flow)+","+"task_name --> " +str(task_name))

    try:
        if str(task_flow) in task_flow_names:
            logging.info("MDS Integration task started !!! --> "+ str(task_flow)+" "+str(task_name))
            result = config_mds(task_flow,task_name)
            logging.info("MDS Integration task Completed !!! --> "+str(task_flow))
        else:
            raise Exception("Unknown task_flow name --> "+ str(task_flow))

    except Exception as e:
        print("initiate_integration.py : MDS Integration task failed for --> ",str(task_flow))
        logging.info("MDS Integration task Failed !!! --> "+ str(task_flow)+" "+str(task_name)) 
        print(e)
        raise Exception(str(e))
    return result

if __name__ == "__main__":
  app.run(host='0.0.0.0', port=8080,debug=True)
