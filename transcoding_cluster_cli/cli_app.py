
import argparse
import socket
from transcoding_cluster import client
from transcoding_cluster import task

from .work_executor import WorkExecutor
from .task_manager import TaskManager
from .worker_manager import WorkerManager

class UsageError( Exception ):
    pass

class CliApp( object ):
    
    def __init__(self):
        self.apiBaseUrl = None
        self.workerId = None
        self.args = None
        self.client = None
    
    def parseArgs(self):
        parser = argparse.ArgumentParser(description='Transcoding Cluster CLI')
        parser.add_argument('-s','--api-url', action="store", dest="api_url", default=None,
                            metavar='URL', help='URL to API server')
    
        subparsers = parser.add_subparsers( dest='subcommand' )
        
        taskP = subparsers.add_parser('task', help='Task commands' )
        taskP.add_argument('-a','--affinity', action="store", dest="task_affinity", default=None,
                            metavar='WORKERID', help='Set the affinity of the task to WORKERID')
        taskP.add_argument('-p','--priority', action="store", dest="task_priority", default=None, type=int,
                            metavar='N', help='Set the task priority')
        taskP.add_argument('-d','--dependency', action="append", dest="task_dependency", default=[], type=int,
                            metavar='TASKID', help='Set task with ID TASKID as dependency. Use multiple times for multiple dependencies. When used with -u, dependencies are only added.')
        taskP.add_argument('-c','--command', action="store", dest="task_command", default=None,
                            metavar='CMD', help='Set the command of the task.')
        taskP.add_argument('-R','--remove-dependency', action="store_true", dest="task_dependency_remove", default=False,
                            help='When updating a task, remove the listed dependencies instead of adding them.')
        taskP.add_argument('-o', '--output-format', action="store", dest="describe_format", choices=["json",] , default=None,
                            metavar='FMT', help='Output format. Default is human readable.')
        taskP.add_argument('-q', '--quiet', action="store_true", dest="task_quiet", default=False,
                            help='Do not output the taks infos after update or creation.')

        taskGr = taskP.add_mutually_exclusive_group(required=True)
        taskGr.add_argument('-n','--new', action="store_true", dest="task_new", default=False,
                            help='Create a new task')
        taskGr.add_argument('-u','--update', action="store", type=int, dest="task_id",
                            metavar='TASKID', help='Update task with ID TASKID')

        descP = subparsers.add_parser('describe', help='Describe ressources like tasks or workers' )
        descP.add_argument('type', action="store", default=None, choices=["task","worker"] , 
                            metavar='RESS', help='Ressource type to describe')
        descP.add_argument('id', action="store", default=None, 
                            metavar='ID', help='ID of the ressource to be described.' )
        descP.add_argument('-o', '--output-format', action="store", dest="describe_format", choices=["json",] , default=None,
                            metavar='FMT', help='Output format. Default is human readable.')

        getP = subparsers.add_parser('get', help='Lists ressources like tasks or workers' )
        getP.add_argument('type', action="store", default=None, choices=["task","worker"] , 
                            metavar='RESS', help='Ressource type to list')
        getP.add_argument('-o', '--output-format', action="store", dest="list_format", choices=["json","list","table"] , default="table",
                            metavar='FMT', help='Output format. Default is table.')

        delP = subparsers.add_parser('delete', help='Delete ressources like tasks or workers' )
        delP.add_argument('type', action="store", default=None, choices=["task","worker"] , 
                            metavar='RESS', help='Ressource type to delete')
        delP.add_argument('id', action="store", default=None, 
                            metavar='ID', help='ID of the ressource to delete.' )


        workP = subparsers.add_parser('work', help='Work executor client' )
        workP.add_argument('-w', '--worker-id', action="store", dest="worker_id", default=None,
                            metavar='WID', help='Worker ID (name). Defaults to the hostname')
        workP.add_argument('-p', '--poll-freq', action="store", dest="worker_poll", default=500, type=int,
                            metavar='N', help='When there is no task to be schedulede, wait N seconds before again requesting work.')

        workerP = subparsers.add_parser('worker', help='Worker commands' )
        workerP.add_argument('-q', '--quiet', action="store_true", dest="worker_quiet", default=False,
                            help='Do not output the worker infos after update or creation.')
        workerP.add_argument('-o', '--output-format', action="store", dest="describe_format", choices=["json",] , default=None,
                            metavar='FMT', help='Output format. Default is human readable.')

        wdrainGr = workerP.add_mutually_exclusive_group()
        wdrainGr.add_argument('-d','--drain', action="store_true", dest="worker_drain", default=False,
                            help='Drain the worker')
        wdrainGr.add_argument('--no-drain', action="store_true", dest="worker_nodrain", default=False,
                            help='Cancel the drainage of the worker')

        workerGr = workerP.add_mutually_exclusive_group(required=True)
        workerGr.add_argument('-n','--new', action="store", dest="worker_new_id", default=None,
                            metavar='NAME', help='Create a new worker')
        workerGr.add_argument('-u','--update', action="store", dest="worker_update_id",
                            metavar='NAME', help='Update worker')

        servP = subparsers.add_parser( 'server', help='Start developpement server' )
        servP.add_argument('conf', action="store",
                            metavar='FILE.INI', help='Server configuration file')
        servP.add_argument('-H','--host', action="store", dest="server_host", default="0.0.0.0",
                            metavar='NAME', help='Listen on IP NAME defaults to 0.0.0.0' )
        servP.add_argument('-p','--port', action="store", dest="server_port", default=5000, type=int,
                            metavar='PORT', help='Listen on port PORT defaults to 5000' )
    
        self.args = parser.parse_args()
    
    def loadApiClient(self):
        assert self.args != None
        if self.args.subcommand == "server":
            return
        
        if self.args.api_url != None:
            self.apiBaseUrl = self.args.api_url
        if self.apiBaseUrl == None:
            raise UsageError( "You must specify the API URL" )
        self.client = client.TranscodingClusterClient( self.apiBaseUrl )
    
    def dispatchSubcommand( self ):
        assert self.args != None

        if self.args.subcommand == "task":
            if self.args.task_id != None:
                self.updateTask()
            else:
                self.createTask()
        elif self.args.subcommand == "describe":
            if self.args.type == "task":
                self.describeTask()
            elif self.args.type == "worker":
                self.describeWorker()
        elif self.args.subcommand == "get":
            if self.args.type == "task":
                self.listTasks()
            elif self.args.type == "worker":
                self.listWorkers()
        elif self.args.subcommand == "delete":
            if self.args.type == "task":
                self.deleteTask()
            elif self.args.type == "worker":
                self.deleteWorker()
        elif self.args.subcommand == "work":
            self.runWorker()
        elif self.args.subcommand == "worker":
            if self.args.worker_update_id != None:
                self.updateWorker()
            elif self.args.worker_new_id != None:
                self.createWorker()
        elif self.args.subcommand == "server":
            self.runServer()
    
    def createTask( self ):
        assert self.client != None
        mgr = TaskManager( self.client )
        
        data = {
            "affinity" : self.args.task_affinity,
            "priority" : self.args.task_priority,
            "command"  : self.args.task_command,
            "depends"  : self.args.task_dependency
        }
        mgr.create( data )
        
        if not self.args.task_quiet:
            mgr.describe( self.args.describe_format )

    def listTasks(self):
        assert self.client != None
        mgr = TaskManager( self.client )
        mgr.listTasks( self.args.list_format )

    def describeTask(self, taskId=None):
        assert self.client != None
        mgr = TaskManager( self.client )
        mgr.loadTask( self.args.id )
        mgr.describe( self.args.describe_format )
    
    def updateTask( self ):
        assert self.client != None
        mgr = TaskManager( self.client )
        mgr.loadTask( self.args.task_id )
        
        data = {
            "affinity" : self.args.task_affinity,
            "priority" : self.args.task_priority,
            "command"  : self.args.task_command,
            "depends"  : self.args.task_dependency
        }
        mgr.update( data, removeDeps=self.args.task_dependency_remove )
        
        if not self.args.task_quiet:
            mgr.describe( self.args.describe_format )
    
    def deleteTask(self):
        assert self.client != None
        self.client.deleteTask( self.args.id )

    
    #
    #   Worker
    #

    def createWorker( self ):
        assert self.client != None
        mgr = WorkerManager( self.client )
        
        data = {
            "id" : self.args.worker_new_id,
            "drain" : self.args.worker_drain
        }
        mgr.create( data )
        
        if not self.args.worker_quiet:
            mgr.describe( self.args.describe_format )

    def listWorkers(self):
        assert self.client != None
        mgr = WorkerManager( self.client )
        mgr.listWorkers( self.args.list_format )

    def describeWorker(self):
        assert self.client != None
        mgr = WorkerManager( self.client )
        mgr.loadWorker( self.args.id )
        mgr.describe( self.args.describe_format )
    
    def updateWorker( self ):
        assert self.client != None
        mgr = WorkerManager( self.client )
        mgr.loadWorker( self.args.worker_update_id )
        
        data = {"id":None}
        if self.args.worker_drain:
            data["drain"] = True
        elif self.args.worker_nodrain:
            data["drain"] = False
            
        mgr.update( data )
        
        if not self.args.worker_quiet:
            mgr.describe( self.args.describe_format )
    
    def deleteWorker(self):
        assert self.client != None
        self.client.deleteWorker( self.args.id )

    def runWorker(self):
        assert self.client != None
        if self.args.worker_id != None:
            workerId = self.args.worker_id
        elif self.workerId != None:
            workerId = self.workerId
        else:
            workerId = socket.gethostname()
        
        exec = WorkExecutor( self.client, workerId )
        exec.pollWaitTime = self.args.worker_poll
        exec.mainLoop()

    def runServer(self):
        from transcoding_cluster_server import create_app
        app = create_app()
        app.run( host=self.args.server_host, port=self.args.server_port )
