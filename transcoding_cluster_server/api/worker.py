from flask.views import MethodView
from flask import request
from flask import jsonify

import json

from transcoding_cluster import worker
from transcoding_cluster import errors

class WorkerAPI(MethodView):
    
    def __init__(self, db ):
        self.db = db

    def _workerIdToResponse(self, workerId, status_code=200 ):
        with self.db:
            w = self.db.getWorker( workerId )
        if w == None:
            raise errors.ApiNotFoundError("Worker {} not found".format(workerId) )
        r = jsonify( w.toApiDict() )
        r.status_code = status_code
        return r

    def get(self, workerId):
        data = None
        if workerId is None:
            data = []
            with self.db:
                workers = self.db.getWorkers()
            for w in workers:
                data.append( w.toApiDict() )
        else:
            with self.db:
                w = self.db.getWorker( workerId )
            if w != None:
                data = w.toApiDict()
            else:
                raise errors.ApiNotFoundError("Worker not found")
            
        return jsonify( data )

    def post(self, workerId=None):
        # create worker
        if not request.is_json:
            raise errors.ApiUsageError("Must be json mime type")
        data = request.get_json()
        if ("id" not in data) or (data["id"] == None):
            raise errors.ApiUsageError("worker.id must be specified")

        w = worker.Worker()
        w.fromApiDict( data )
        with self.db:
            self.db.insertWorker( w )
        
        return self._workerIdToResponse( w.id, status_code=201)

    def delete(self, workerId):
        w = self.db.getWorker( workerId )
        if w == None:
            raise errors.ApiNotFoundError("Failed to delete Worker: not found")
        with self.db:
            self.db.deleteWorker( w )
        return jsonify(None)

    def put(self, taskId):
        raise errors.ApiUsageError("Not implemented")

    def patch(self, workerId):
        # update worker
        if not request.is_json:
            raise errors.ApiUsageError("Must be json mime type")
        data = request.get_json()
        if "id" in data:
            raise errors.ApiUsageError("worker.id cannot be updated")

        try:
            updateAttrs = worker.Worker.apiKeysToAttributes( data.keys() )
        except KeyError as e:
            raise errors.ApiUsageError("Invalid key: {}".format(e.message) ) from None
        
        with self.db:
            w = self.db.getWorker( workerId )
            if w == None:
                raise errors.ApiNotFoundError("Worker not found")
            
            w.fromApiDict( data )
            self.db.updateWorker(w, updateAttrs )
        
        return self._workerIdToResponse( w.id, status_code=200)
