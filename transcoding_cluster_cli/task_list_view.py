import json

from .table_writer import TableWriter
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
        pass
    
    def show(self, tList):
        if tList == None:
            return
        
        ta = TableWriter()
        headLine = ["ID", "Status", "Prio.", "Worker", "Affinity"] 
        ta.appendRow( headLine )
        
        for t in tList:
            row = []
            row.append( str(t.id) )
            row.append( str(t.status.name) )
            row.append( str(t.priority) )
            row.append( str(t.workerId) )
            row.append( str(t.affinity) )
            ta.appendRow( row )
        
        ta.setConf( 0, None, "heading", True)
        for i in range(len(headLine)):
            ta.setConf( None, i, "wrap", False)

        ta.display()
