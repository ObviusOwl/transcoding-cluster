from transcoding_cluster import task
from .task_view import TaskHumanView, TaskJsonView
from .task_list_view import TaskListListView, TaskListJsonView, TaskListTableView

class TaskManager( object ):
    
    def __init__(self, client):
        self.client = client
        self.task = None
    
    def loadTask( self, taskId ):
        self.task = self.client.getTask( taskId )
    
    def requireTask( self ):
        if self.task == None:
            raise RuntimeError( "Task must be loaded" )

    def listTasks(self, format):
        viewMap = {
            "json" : TaskListJsonView,
            "table": TaskListTableView,
            "list" : TaskListListView
        }
        if format in viewMap.keys():
            listV = viewMap[ format ]()
        else:
            listV = TaskListListView()

        tList = self.client.getTasks()
        if tList != None:
            listV.show( tList )

    def describe( self, format ):
        self.requireTask()
        if format == "json":
            taskV = TaskJsonView()
        else:
            taskV = TaskHumanView()
        taskV.show( self.task )

    
    def updateDependencies( self, depList, remove=False ):
        newDeps = []
        for d in depList:
            if not d in self.task.depends and not remove:
                # updates self.task
                self.client.addTaskDependency(self.task, d)
            elif d in self.task.depends and remove:
                # updates self.task
                self.client.removeTaskDependency(self.task, d)
    
    def updateAttributes( self, data ):
        attrs = []
        if data["affinity"] != None:
            self.task.affinity = data["affinity"]
            attrs.append( "affinity" )
        if data["priority"] != None:
            self.task.priority = data["priority"]
            attrs.append( "priority" )
        if data["command"] != None:
            self.task.command = data["command"]
            attrs.append( "command" )
        return attrs
        
    def update( self, data, removeDeps=False ):
        self.requireTask()
        attrs = self.updateAttributes( data )

        self.task = self.client.updateTask( self.task, attrs )
        if "depends" in data:
            self.updateDependencies( data["depends"], removeDeps )

    def create( self, data ):
        self.task = task.Task()
        self.updateAttributes( data )
        
        self.task = self.client.createTask( self.task )
        if "depends" in data:
            self.updateDependencies( data["depends"], False )
