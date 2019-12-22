import json
import os
import configparser
from flask import Flask
from flask import jsonify

from .db import Database
from .config import Config
from transcoding_cluster import errors

from .api.task import TaskAPI
from .api.task_dependencies import TaskDependenciesAPI
from .api.worker import WorkerAPI
from .api.scheduler_request import SchedulerRequestAPI

def create_app():
    # TODO: load DEV or PROD config depending on flask env
    cnf = Config()
    cnf.loadFile( os.environ['TRANSCODING_CLUSTER_CONFIG'] )
    db = Database( cnf )
    

    app = Flask(__name__, instance_relative_config=True)
    @app.errorhandler(errors.ApiError)
    def handle_api_error(error):
        r = jsonify( error.toApiDict() )
        r.status_code = error.statusCode
        return r

    taskView = TaskAPI.as_view('task_api', db)
    app.add_url_rule('/api/tasks/', defaults={'taskId': None}, view_func=taskView, methods=['GET','POST'])
    app.add_url_rule('/api/tasks/<int:taskId>', view_func=taskView, methods=['GET', 'PUT', 'PATCH', 'DELETE'])

    taskDepView = TaskDependenciesAPI.as_view('task_dependencies_api', db)
    app.add_url_rule('/api/tasks/<int:taskId>/dependencies', defaults={'depId': None}, view_func=taskDepView, methods=['GET','POST'])
    app.add_url_rule('/api/tasks/<int:taskId>/dependencies/<int:depId>', view_func=taskDepView, methods=['GET', 'PUT', 'PATCH', 'DELETE'])

    workerView = WorkerAPI.as_view('worker_api', db)
    app.add_url_rule('/api/workers/', defaults={'workerId': None}, view_func=workerView, methods=['GET','POST'])
    app.add_url_rule('/api/workers/<string:workerId>', view_func=workerView, methods=['GET', 'PUT', 'PATCH', 'DELETE'])

    sReqView = SchedulerRequestAPI.as_view('scheduler_request_api', db)
    app.add_url_rule('/api/scheduler/request/', view_func=sReqView, methods=['POST',])
    
    return app