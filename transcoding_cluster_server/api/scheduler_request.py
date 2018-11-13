from flask.views import MethodView
from flask import request
from flask import jsonify

import json

from transcoding_cluster_server import scheduler
from transcoding_cluster import errors

class SchedulerRequestAPI(MethodView):
    
    def __init__(self, db ):
        self.db = db
    
    def get(self):
        raise ApiForbiddenError("Not GET request allowed")

    def post(self, taskId=None):
        if not request.is_json:
            raise errors.ApiUsageError("Must be json mime type")
        data = request.get_json()
        if "worker_id" not in data:
            raise errors.ApiUsageError("You must specify request.worker_id")
        doPeek = False
        if "peek" in data:
            doPeek = bool( data["peek"] )
        
        sched = scheduler.Scheduler( self.db )
        try:
            with sched:
                t = sched.getNextTask( data["worker_id"], peek=doPeek )
        except errors.SchedulerError as e:
            raise errors.ApiError( str(e) )
        if t == None:
            raise errors.ApiNotFoundError("No task to be scheduled")
        
        return jsonify( t.toApiDict() )

    def delete(self, taskId):
        raise errors.ApiUsageError("Not implemented yet")

    def put(self, taskId):
        raise errors.ApiForbiddenError("Not PUT request allowed")

    def patch(self, taskId):
        raise errors.ApiForbiddenError("Not PATCH request allowed")
