
class ApiTypeError(Exception):
    pass
    
class DatabaseError(Exception):
    pass

class SchedulerError(Exception):
    pass

class ApiError(Exception):

    def __init__(self, message, statusCode=500, data=None):
        super().__init__(self)
        self.message = message
        self.statusCode = statusCode
        self.data = data

    def toApiDict(self):
        r = { "message": self.message, }
        if self.data != None:
            for k,v in self.data.items:
                r[k] = v
        return r
    
    def __str__(self):
        return "{}({}): {}".format( type(self).__name__, self.statusCode, self.message )

class ApiNotFoundError(ApiError):
    def __init__(self, message, statusCode=404, data=None):
        super().__init__( message, statusCode, data)

class ApiUsageError(ApiError):
    def __init__(self, message, statusCode=400, data=None):
        super().__init__( message, statusCode, data)

class ApiForbiddenError(ApiError):
    def __init__(self, message, statusCode=403, data=None):
        super().__init__( message, statusCode, data)
