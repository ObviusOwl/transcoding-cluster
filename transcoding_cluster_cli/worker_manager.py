#from transcoding_cluster import task
#from .task_view import TaskHumanView, TaskJsonView
#from .task_list_view import TaskListListView, TaskListJsonView, TaskListTableView

from transcoding_cluster import worker
from .worker_view import WorkerHumanView, WorkerJsonView
from .worker_list_view import WorkerListListView, WorkerListJsonView, WorkerListTableView

class WorkerManager( object ):
    
    def __init__(self, client):
        self.client = client
        self.worker = None
    
    def loadWorker( self, workerId ):
        self.worker = self.client.getWorker( workerId )
    
    def requireWorker( self ):
        if self.worker == None:
            raise RuntimeError( "Worker must be loaded" )

    def listWorkers(self, format):
        viewMap = {
            "json" : WorkerListJsonView,
            "table": WorkerListTableView,
            "list" : WorkerListListView
        }
        if format in viewMap.keys():
            listV = viewMap[ format ]()
        else:
            listV = WorkerListListView()

        wList = self.client.getWorkers()
        if wList != None:
            listV.show( wList )

    def describe( self, format ):
        self.requireWorker()
        if format == "json":
            workerV = WorkerJsonView()
        else:
            workerV = WorkerHumanView()
        workerV.show( self.worker )
    
    def updateAttributes( self, data ):
        attrs = []
        if data["drain"] != None:
            self.worker.drain = data["drain"]
            attrs.append( "drain" )
        if data["id"] != None:
            self.worker.id = data["id"]
            attrs.append( "id" )
        return attrs
        
    def update( self, data ):
        self.requireWorker()
        attrs = self.updateAttributes( data )

        self.worker = self.client.updateWorker( self.worker, attrs )

    def create( self, data ):
        self.worker = worker.Worker()
        self.updateAttributes( data )
        
        self.worker = self.client.createWorker( self.worker )
