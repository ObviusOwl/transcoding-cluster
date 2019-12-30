import json

from transcoding_cluster.task import TaskStatus
from .table_writer import TableWriter, AnsiColor
from .task_view import TaskHumanView

class TaskListListView( object ):
    
    def __init__(self):
        pass

    def show(self, tList):
        if tList == None:
            return

        tView = TaskHumanView()
        for i in range(len(tList)):
            if i != 0:
                print( "\n---\n" )
            tView.show( tList[i] )

class TaskListJsonView( object ):
    
    def __init__(self):
        pass
    
    def show(self, tList):
        if tList == None:
            return
        data = []
        for t in tList:
            data.append( t.toApiDict() )
        jsonData = json.dumps(data, indent=2)
        print( jsonData )

class TaskListTableView( object ):
    
    def __init__(self):
        self.statusColorMap = { 
            TaskStatus.schedulable: AnsiColor.default,
            TaskStatus.dispatched: AnsiColor.yellow,
            TaskStatus.done: AnsiColor.green,
            TaskStatus.failed: AnsiColor.red,
        }
    
    def show(self, tList):
        if tList == None:
            return
        
        ta = TableWriter()
        headLine = ["ID", "Status", "Prio.", "Worker", "Affinity", "Command"] 
        ta.appendRow( headLine )
        
        for t in tList:
            row = []
            row.append( str(t.id) )
            row.append( str(t.status.name) )
            row.append( str(t.priority) )
            row.append( str(t.workerId) )
            row.append( str(t.affinity) )
            row.append( str(t.command) )
            row = ta.appendRow( row )

            if t.status in self.statusColorMap:
                row.color = self.statusColorMap[ t.status ]
        
        ta.table.rows[ 0 ].isHeader = True
        for col in ta.table.columns:
            col.textWrap = False
        ta.table.columns[ 5 ].textWrap = True
        
        ta.display()
