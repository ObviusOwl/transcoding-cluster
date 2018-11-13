import json

class TaskHumanView( object ):
    
    def __init__(self):
        pass
    
    def printAttrLine(self, t, attr, name):
        print( name + ": "+ str(getattr(t, attr)) )
    
    @staticmethod
    def getDependencyStr(t):
        if t.depends == None or len(t.depends) == 0:
            return "None"
        data = []
        for id in t.depends:
            data.append( str(id) )
        return ", ".join( data )
    
    def show(self, t):
        if t == None:
            return
        
        self.printAttrLine(t, "id", "ID" )
        print( "Status: "+ str(t.status.name) )
        self.printAttrLine(t, "priority", "Priority" )
        self.printAttrLine(t, "affinity", "Affinity" )
        self.printAttrLine(t, "workerId", "Worker" )
        print( "Dependencies: "+ self.getDependencyStr(t) )
        
        print( "\nCommand:" )
        print( t.command )

class TaskJsonView( object ):
    
    def __init__(self):
        pass
    
    def show(self, t):
        if t == None:
            return
        data = t.toApiDict()
        jsonData = json.dumps(data, indent=2)
        print( jsonData )