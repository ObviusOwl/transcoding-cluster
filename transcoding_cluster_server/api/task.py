from flask.views import MethodView
from flask import request
from flask import jsonify

import json

from transcoding_cluster import task
from transcoding_cluster import errors

class TaskAPI(MethodView):
    
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
    
    def get(self, taskId):
        data = None
        if taskId is None:
            # get all tasks
            data = []
            with self.db:
                tasks = self.db.getTasks()
            for t in tasks:
                data.append( t.toApiDict() )
        else:
            # get specific task
            with self.db:
                t = self.db.getTask( taskId )
            if t != None:
                data = t.toApiDict()
            else:
                raise errors.ApiNotFoundError("Task not found")
            
        return jsonify( data )

    def post(self, taskId=None):
        # create task
        if not request.is_json:
            raise errors.ApiUsageError("Must be json mime type")
        data = request.get_json()
        if "id" in data:
            raise errors.ApiUsageError("You must not specify task.id")
        
        t = task.Task()
        t.fromApiDict( data )
        
        with self.db:
            taskId = self.db.insertTask( t )
        
        return self._taskIdToResponse( taskId, status_code=201) 

    def delete(self, taskId):
        with self.db:
            t = self.db.getTask( taskId )
            if t == None:
                raise errors.ApiNotFoundError("Failed to delete Task: not found")
            self.db.deleteTask( t )
        return jsonify(None)

    def put(self, taskId):
        # replace task
        raise errors.ApiUsageError("Not implemented")

    def patch(self, taskId):
        # update task
        if not request.is_json:
            raise errors.ApiUsageError("Must be json mime type")
        data = request.get_json()
        if "id" in data:
            raise errors.ApiUsageError("task.id cannot be updated")
        if "depends" in data:
            raise errors.ApiUsageError("task.depends cannot be updated, use the /tasks/1/depends endpoint")

        try:
            updateAttrs = task.Task.apiKeysToAttributes( data.keys() )
        except KeyError as e:
            raise errors.ApiUsageError("Invalid key: {}".format(e.message) ) from None
        
        with self.db:
            t = self.db.getTask( taskId )
            if t == None:
                raise errors.ApiNotFoundError("Task not found")
            
            t.fromApiDict( data )
            self.db.updateTask(t, updateAttrs )
        
        return self._taskIdToResponse( taskId, status_code=200) 
