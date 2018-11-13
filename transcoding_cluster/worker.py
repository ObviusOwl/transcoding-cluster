
from .errors import ApiTypeError

class Worker( object ):
    
    def __init__(self):
        self.id = None # name
        self.drain = False


    @staticmethod
    def apiKeysToAttributes(keys, invert=False):
        kMap = {
            # API => attr
            "id": "id",
            "drain": "drain"
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
        return Worker.apiKeysToAttributes(attrs, invert=True )

    
    def toApiDict( self ):
        return {
            "id" : self.id,
            "drain": self.drain
        }
    
    def fromApiDict( self, data ):
        if "id" in data:
            if not (isinstance( data["id"], str) or data["id"] == None):
                raise ApiTypeError( "worker.id must be string" )
            self.id = data["id"]
        if "drain" in data:
            self.drain = bool( data["drain"] )
