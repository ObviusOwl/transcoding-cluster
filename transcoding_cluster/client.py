
import requests
import os

from .errors import ApiTypeError, ApiError
from .task import Task
from .worker import Worker

class TranscodingClusterClient( object ):
    
    def __init__( self, apiBaseUrl ):
        self.baseUrl = apiBaseUrl
    
    def raiseApiError(self, resp):
        if resp.ok == True:
            return
        try:
            data = resp.json()
        except ValueError:
            resp.raise_for_status()
        if "message" in data:
            raise ApiError( data["message"], resp.status_code, data)
        
    def responseToTask(self, resp):
        data = resp.json()
        t = Task()
        t.fromApiDict( data )
        return t
    
    def responseToWorker(self, resp):
        data = resp.json()
        w = Worker()
        w.fromApiDict( data )
        return w

    def getTask( self, taskId ):
        apiPath = "tasks/{}".format( taskId )
        url = os.path.join( self.baseUrl, apiPath )
        
        r = requests.get( url )
        self.raiseApiError(r)
        return self.responseToTask( r )
    
    def getTasks( self ):
        apiPath = "tasks/"
        url = os.path.join( self.baseUrl, apiPath )
        
        r = requests.get( url )
        self.raiseApiError(r)

        data = r.json()
        ret = []
        for d in data:
            t = Task()
            t.fromApiDict( d )
            ret.append( t )
        return ret
    
    def updateTask(self, t, attributes ):
        data = t.toApiDict()

        if "depends" in data:
            # not allowed to update with PATCH
            del data["depends"]
        if "id" in data:
            del data["id"]

        data2 = {}
        for k in t.attributesToApiKeys( attributes ):
            if k in data.keys():
                data2[ k ] = data[ k ]
        data = data2
        
        apiPath = "tasks/{}".format( t.id )
        url = os.path.join( self.baseUrl, apiPath )
        
        r = requests.patch( url, json=data )
        self.raiseApiError(r)
        return self.responseToTask( r )
    
    def createTask(self, t):
        data = t.toApiDict()

        if "depends" in data:
            # not allowed to update with PATCH
            del data["depends"]
        if "id" in data:
            del data["id"]
        
        apiPath = "tasks/"
        url = os.path.join( self.baseUrl, apiPath )
        
        r = requests.post( url, json=data )
        self.raiseApiError(r)
        return self.responseToTask( r )
    
    def requestTask(self, workerId, peek=False ):
        apiPath = "scheduler/request/"
        url = os.path.join( self.baseUrl, apiPath )
        data = { 'worker_id': workerId, 'peek':peek }
        
        r = requests.post( url, json=data )
        self.raiseApiError(r)
        return self.responseToTask( r )
    
    def addTaskDependency(self, t, depId ):
        apiPath = "tasks/{}/dependencies".format( t.id )
        url = os.path.join( self.baseUrl, apiPath )
        data = {"id":depId}
        
        r = requests.post( url, json=data )
        self.raiseApiError(r)
        if not depId in t.depends:
            t.depends.append(depId)

    def removeTaskDependency(self, t, depId ):
        apiPath = "tasks/{}/dependencies/{}".format( t.id, depId )
        url = os.path.join( self.baseUrl, apiPath )
        
        r = requests.delete( url )
        self.raiseApiError(r)
        if depId in t.depends:
            t.depends.remove( depId )

    def deleteTask(self, taskId ):
        apiPath = "tasks/{}".format( taskId )
        url = os.path.join( self.baseUrl, apiPath )
        
        r = requests.delete( url )
        self.raiseApiError(r)

    #
    #   Worker
    #


    def getWorker( self, workerId ):
        apiPath = "workers/{}".format( workerId )
        url = os.path.join( self.baseUrl, apiPath )
        
        r = requests.get( url )
        self.raiseApiError(r)
        return self.responseToWorker( r )
    
    def getWorkers( self ):
        apiPath = "workers/"
        url = os.path.join( self.baseUrl, apiPath )
        
        r = requests.get( url )
        self.raiseApiError(r)

        data = r.json()
        ret = []
        for d in data:
            w = Worker()
            w.fromApiDict( d )
            ret.append( w )
        return ret
    
    def updateWorker(self, w, attributes ):
        data = w.toApiDict()
        if "id" in data:
            del data["id"]

        data2 = {}
        for k in w.attributesToApiKeys( attributes ):
            if k in data.keys():
                data2[ k ] = data[ k ]
        data = data2
        
        apiPath = "workers/{}".format( w.id )
        url = os.path.join( self.baseUrl, apiPath )
        
        r = requests.patch( url, json=data )
        self.raiseApiError(r)
        return self.responseToWorker( r )
        
    def createWorker(self, w):
        data = w.toApiDict()

        apiPath = "workers/"
        url = os.path.join( self.baseUrl, apiPath )
        
        r = requests.post( url, json=data )
        self.raiseApiError(r)
        return self.responseToWorker( r )

    def deleteWorker(self, workerId ):
        apiPath = "workers/{}".format( workerId )
        url = os.path.join( self.baseUrl, apiPath )
        
        r = requests.delete( url )
        self.raiseApiError(r)
