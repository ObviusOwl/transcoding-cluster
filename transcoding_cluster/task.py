from enum import Enum

from .errors import ApiTypeError

class TaskStatus(Enum):
    schedulable = 0
    dispatched = 1
    done = 2
    failed = 3

class Task( object ):
    
    def __init__(self):
        self.depends = []
        self.id = None
        self.command = ""
        self.workerId = None
        self.status = TaskStatus.schedulable
        self.priority = 0
        self.affinity = None
    
    @staticmethod
    def apiKeysToAttributes(keys, invert=False):
        kMap = {
            # API => attr
            "id": "id",
            "depends": "depends",
            "command": "command",
            "worker_id": "workerId",
            "status": "status",
            "priority": "priority",
            "affinity": "affinity"
        }
        if invert:
            kMapInv = {}
            for k,v in kMap.items():
                kMapInv[v] = k
            kMap = kMapInv
            
        ret = []
        for k in keys:
            if not k in kMap.keys():
                raise KeyError(k)
            ret.append( kMap[k] )
        return ret
    
    @staticmethod
    def attributesToApiKeys(attrs):
        return Task.apiKeysToAttributes(attrs, invert=True )
    
    def toApiDict( self ):
        keys = ["id","depends","command","worker_id", "priority", "affinity" ]
        attrs = self.apiKeysToAttributes(keys)
        ret = {}
        
        # simple values
        for i in range(len(keys)):
            ret[ keys[i] ] = getattr(self, attrs[i] )

        # status enum: use names
        ret[ "status" ] = self.status.name
        return ret
    
    def fromApiDict( self, data ):
        if "id" in data:
            if not (isinstance(data["id"], int) or data["id"] == None):
                raise ApiTypeError( "task.id must be integer" )
            self.id = data["id"]

        if "command" in data:
            if not (isinstance( data["command"], str) or data["command"] == None ):
                raise ApiTypeError( "task.command must be string" )
            self.command = data["command"]

        if "depends" in data:
            if data["depends"] != None:
                try:
                    for d in data["depends"]:
                        if not isinstance( d, int):
                            raise ApiTypeError( "task.depends must be iterable of int" )
                except TypeError:
                    raise ApiTypeError( "task.depends must be iterable of int" ) from None
            self.depends = data["depends"]
        
        if "worker_id" in data:
            if not (isinstance( data["worker_id"], str) or data["worker_id"] == None):
                raise ApiTypeError( "task.worker_id must be string" )
            self.workerId = data["worker_id"]

        if "priority" in data:
            if not (isinstance( data["priority"], int) or data["priority"] == None):
                raise ApiTypeError( "task.priority must be int" )
            self.priority = data["priority"]

        if "affinity" in data:
            if not (isinstance( data["affinity"], str) or data["affinity"] == None):
                raise ApiTypeError( "task.affinity must be str" )
            self.affinity = data["affinity"]

        if "status" in data:
            statStr = data["status"]
            if not isinstance( statStr, str):
                raise ApiTypeError( "task.status must be str" )
            try:
                self.status = TaskStatus[ statStr ]
            except KeyError:
                raise ApiTypeError("'{}' is not a valid status for task.status".format(statStr) ) from None
        
        