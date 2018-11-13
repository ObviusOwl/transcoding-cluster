import json

class WorkerHumanView( object ):
    
    def __init__(self):
        pass
    
    def printAttrLine(self, t, attr, name):
        print( name + ": "+ str(getattr(t, attr)) )
    
    def show(self, t):
        if t == None:
            return
        
        self.printAttrLine(t, "id", "ID" )
        self.printAttrLine(t, "drain", "Drain" )

class WorkerJsonView( object ):
    
    def __init__(self):
        pass
    
    def show(self, w):
        if w == None:
            return
        data = w.toApiDict()
        jsonData = json.dumps(data, indent=2)
        print( jsonData )