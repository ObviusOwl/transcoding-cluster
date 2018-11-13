import json

from .table_writer import TableWriter
from .worker_view import WorkerHumanView

class WorkerListListView( object ):
    
    def __init__(self):
        pass

    def show(self, wList):
        if wList == None:
            return

        wView = WorkerHumanView()
        for i in range(len(wList)):
            if i != 0:
                print( "\n---\n" )
            wView.show( wList[i] )

class WorkerListJsonView( object ):
    
    def __init__(self):
        pass
    
    def show(self, wList):
        if wList == None:
            return
        data = []
        for w in wList:
            data.append( w.toApiDict() )
        jsonData = json.dumps(data, indent=2)
        print( jsonData )

class WorkerListTableView( object ):
    
    def __init__(self):
        pass
    
    def show(self, wList):
        if wList == None:
            return
        
        ta = TableWriter()
        headLine = ["ID", "Drain"] 
        ta.appendRow( headLine )
        
        for w in wList:
            row = []
            row.append( str(w.id) )
            row.append( str(w.drain) )
            ta.appendRow( row )
        
        ta.setConf( 0, None, "heading", True)
        for i in range(len(headLine)):
            ta.setConf( None, i, "wrap", False)

        ta.display()
