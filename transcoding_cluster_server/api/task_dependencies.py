from flask.views import MethodView
from flask import request
from flask import jsonify

import json

from transcoding_cluster import task
from transcoding_cluster import errors

class TaskDependenciesAPI(MethodView):
    
    def __init__(self, db ):
        self.db = db
    
    def _taskIdToResponse(self, taskId, status_code=200 ):
        with self.db:
            t = self.db.getTask( taskId )
        if t == None:
            raise errors.ApiNotFoundError("Task {} not found".format(taskId) )
        r = jsonify( t.toApiDict() )
        r.status_code = status_code
        return r

    def get(self, taskId=None, depId=None):
        data = None
        with self.db:
            t = self.db.getTask( taskId )
        if t == None:
            raise errors.ApiNotFoundError("Task not found")

        if depId is None:
            # get all dependencies
            data = []
            for depId in t.depends:
                with self.db:
                    dt = self.db.getTask( depId )
                if dt != None:
                    data.append( dt.toApiDict() )
        else:
            # get specific task
            with self.db:
                t = self.db.getTask( depId )
            if t != None:
                data = t.toApiDict()
            else:
                raise errors.ApiNotFoundError("Task not a dependency")
            
        return jsonify( data )

    def post(self, taskId=None, depId=None):
        # create dependency
        if not request.is_json:
            raise errors.ApiUsageError("Must be json mime type")
        data = request.get_json()
        if "id" not in data:
            raise errors.ApiUsageError("You must specify task.id")

        with self.db:
            t = self.db.getTask( taskId )
        if t == None:
            raise errors.ApiNotFoundError("Task not found")
        
        dt = task.Task()
        dt.fromApiDict( data )
        
        try:
            with self.db:
                self.db.addTaskDependency( t.id, dt.id )
        except errors.DatabaseError as e:
            raise errors.ApiUsageError( str(e) ) from None
        
        return self._taskIdToResponse(dt.id, status_code=201)

    def delete(self, taskId=None, depId=None):
        with self.db:
            t = self.db.getTask( taskId )
        if t == None:
            raise errors.ApiNotFoundError("Task not found")
        with self.db:
            self.db.removeTaskDependency( t.id, depId)
        return jsonify(None)

    def put(self, taskId=None, depId=None):
        # replace task
        raise errors.ApiUsageError("Not implemented")

    def patch(self, taskId=None, depId=None):
        # update task
        raise errors.ApiUsageError("Not implemented")
