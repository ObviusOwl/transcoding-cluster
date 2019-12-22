import mysql.connector
from threading import RLock

from transcoding_cluster import errors
from transcoding_cluster import task
from transcoding_cluster import worker

class Database( object ):
    
    def __init__(self, config ):
        self._cnf = config
        
        try:
            poolSize = int( self._cnf[ "db_pool_size" ] )
        except (KeyError, ValueError):
            poolSize = 10
        
        opts = { "host": config["db_host"], "port": config["db_port"], 
                "user": config["db_user"], "password": config["db_password"], 
                "database": config["db_name"], "pool_size": poolSize }
        
        self.dbh = mysql.connector.connect( **opts )
        
        self.lock = RLock()
        self.autoCommit = True

    def __enter__(self):
        self.lock.acquire()
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.lock.release()

    def _taskFromRow( self, data ):
        if data == None:
            return None
        t = task.Task()
        if "id" in data:
            t.id = data["id"]
        if "command" in data:
            t.command = data["command"]
        if "worker_id" in data:
            t.workerId = data["worker_id"]
        if "priority" in data:
            t.priority = data["priority"]
        if "affinity" in data:
            t.affinity = data["affinity"]
        if "status" in data:
            try:
                t.status = task.TaskStatus( data["status"] )
            except ValueError:
                pass
        return t

    def _workerFromRow( self, data ):
        if data == None:
            return None
        w = worker.Worker()
        if "id" in data:
            w.id = data["id"]
        if "drain" in data:
            w.drain = bool( data["drain"] )
        return w
    
    def _rowToDict( self, names, row):
        data = None
        if row != None:
            data = dict( zip(names, row) )
        return data
    
    def _requireTaskId(self, t):
        if t.id == None:
            raise errors.DatabaseError( "task.id is None" )

    def _requireWorkerId(self, w):
        if w.id == None:
            raise errors.DatabaseError( "worker.id is None" )
    
    #
    #   Task
    #
    
    def loadTaskDependencies(self, t):
        self._requireTaskId(t)
        data = {}
        quer = ("SELECT dep_id FROM task_dependencies WHERE task_id = %s")
        params = (t.id,)
        
        try:
            cur = self.dbh.cursor()
            cur.execute(quer, params )
            for r in cur:
                t.depends.append( r[0] )
        finally:
            if self.autoCommit:
                self.dbh.commit()
            cur.close()
        
    
    def getTask(self, taskId ):
        data = {}
        quer = ("SELECT * FROM tasks WHERE id = %s")
        params = (taskId,)
        
        try:
            cur = self.dbh.cursor()
            cur.execute(quer, params )
            data = self._rowToDict( cur.column_names, cur.fetchone() )
            print(data)
        finally:
            if self.autoCommit:
                self.dbh.commit()
            cur.close()

        t = self._taskFromRow( data )
        if t != None:
            self.loadTaskDependencies(t)
        return t

    def getTasks(self, offset=0, count=None ):
        data = []
        if count == None:
            quer = ("SELECT * FROM tasks")
            params = None
        else:
            quer = ("SELECT * FROM tasks LIMIT %s %s")
            params = (offset, count)
        
        try:
            cur = self.dbh.cursor()
            cur.execute(quer, params )
            for r in cur:
                t = self._taskFromRow( self._rowToDict( cur.column_names, r ) )
                if t != None:
                    data.append( t )
        finally:
            if self.autoCommit:
                self.dbh.commit()
            cur.close()
        
        for t in data:
            self.loadTaskDependencies(t)
        
        return data

    def queryTasks(self, quer, params ):
        data = []
        
        try:
            cur = self.dbh.cursor()
            cur.execute(quer, params )
            for r in cur:
                t = self._taskFromRow( self._rowToDict( cur.column_names, r ) )
                if t != None:
                    data.append( t )
        finally:
            if self.autoCommit:
                self.dbh.commit()
            cur.close()

        for t in data:
            self.loadTaskDependencies(t)
        
        return data
    
    def insertTask(self, t):
        quer = ("INSERT INTO tasks (command, worker_id, priority, affinity, status)"
                " VALUES (%(cmd)s, %(wk)s, %(prio)s, %(aff)s, %(stat)s)")
        params = { "cmd" : t.command, "wk": t.workerId, 
                  "prio": t.priority, "aff":t.affinity, "stat":t.status.value}
        taskId = None
        
        try:
            cur = self.dbh.cursor()
            cur.execute(quer, params )
            self.dbh.commit()
            taskId = cur.lastrowid
            t.id = taskId
            if self.autoCommit:
                self.dbh.commit()
        except Exception as e:
            if self.autoCommit:
                self.dbh.rollback()
            raise e from None
        finally:
            cur.close()
        
        return taskId
    
    def deleteTask(self, t ):
        self._requireTaskId(t)

        quer = "DELETE FROM tasks WHERE id = %s"
        params = (t.id,)
        
        try:
            cur = self.dbh.cursor()
            cur.execute(quer, params )
            if self.autoCommit:
                self.dbh.commit()
        except Exception as e:
            if self.autoCommit:
                self.dbh.rollback()
            raise e from None
        finally:
            cur.close()
        
        return None
    
    def updateTask(self, t, attributes):
        self._requireTaskId(t)
        if "depends" in attributes:
            raise NotImplementedError()
        if len( attributes ) == 0:
            return
            
        # attribute => column
        simpleUpdateMap = {"command": "command", "workerId": "worker_id", 
                           "priority":"priority", "affinity": "affinity"}
        quer = "UPDATE tasks SET"
        params = []
        
        if "status" in attributes:
            quer = quer + " status = %s,"
            params.append( t.status.value )
        
        for attr in attributes:
            if attr in simpleUpdateMap.keys():
                quer = quer + " {} = %s,".format( simpleUpdateMap[attr] )
                params.append( getattr(t, attr) )
        
        quer = quer.rstrip(",") + " WHERE id = %s"
        params.append( t.id )
        
        try:
            cur = self.dbh.cursor()
            cur.execute(quer, params )
            if self.autoCommit:
                self.dbh.commit()
        except Exception as e:
            if self.autoCommit:
                self.dbh.rollback()
            raise e from None
        finally:
            cur.close()

    def addTaskDependency(self, taskId, depId):
        quer = ("INSERT INTO task_dependencies (task_id, dep_id)"
                " VALUES (%(tid)s, %(depid)s)")
        params = { "tid" : taskId, "depid": depId}
        
        try:
            cur = self.dbh.cursor()
            cur.execute(quer, params )
            if self.autoCommit:
                self.dbh.commit()
        except mysql.connector.errors.IntegrityError:
            raise errors.DatabaseError( "Dependency exists." )
        except Exception as e:
            if self.autoCommit:
                self.dbh.rollback()
            raise e from None
        finally:
            cur.close()

    def removeTaskDependency(self, taskId, depId):
        quer = "DELETE FROM task_dependencies WHERE task_id = %s AND dep_id = %s"
        params = (taskId,depId)
        
        try:
            cur = self.dbh.cursor()
            cur.execute(quer, params )
            if self.autoCommit:
                self.dbh.commit()
        except Exception as e:
            if self.autoCommit:
                self.dbh.rollback()
            raise e from None
        finally:
            cur.close()
        
        return None

    #
    #   Worker
    #

    def getWorker(self, workerId ):
        data = {}
        quer = ("SELECT * FROM workers WHERE id = %s")
        params = (workerId,)
        
        try:
            cur = self.dbh.cursor()
            cur.execute(quer, params )
            data = self._rowToDict( cur.column_names, cur.fetchone() )
        finally:
            if self.autoCommit:
                self.dbh.commit()
            cur.close()
        
        return self._workerFromRow( data )

    def getWorkers(self, offset=0, count=None ):
        data = []
        if count == None:
            quer = ("SELECT * FROM workers")
            params = None
        else:
            quer = ("SELECT * FROM workers LIMIT %s %s")
            params = (offset, count)
        
        try:
            cur = self.dbh.cursor()
            cur.execute(quer, params )
            for r in cur:
                row = self._rowToDict( cur.column_names, r )
                data.append( self._workerFromRow( row ) )
        finally:
            if self.autoCommit:
                self.dbh.commit()
            cur.close()
        
        return data
    
    def insertWorker(self, w):
        quer = "INSERT INTO workers (id, drain) VALUES (%(id)s, %(dr)s)"
        params = { "id" : w.id, "dr": w.drain}
        
        try:
            cur = self.dbh.cursor()
            cur.execute(quer, params )
            if self.autoCommit:
                self.dbh.commit()
        except Exception as e:
            if self.autoCommit:
                self.dbh.rollback()
            raise e from None
        finally:
            cur.close()
        
        return w
    
    def deleteWorker(self, w ):
        if w.id == None:
            raise errors.DatabaseError( "worker.id is None" )

        quer = "DELETE FROM workers WHERE id = %s"
        params = (w.id,)
        
        try:
            cur = self.dbh.cursor()
            cur.execute(quer, params )
            if self.autoCommit:
                self.dbh.commit()
        except Exception as e:
            if self.autoCommit:
                self.dbh.rollback()
            raise e from None
        finally:
            cur.close()
        
        return None
    
    def updateWorker(self, w, attributes):
        self._requireWorkerId(w)
        if len( attributes ) == 0:
            return
            
        # attribute => column
        simpleUpdateMap = {"drain": "drain", }
        quer = "UPDATE workers SET"
        params = []
        
        for attr in attributes:
            if attr in simpleUpdateMap.keys():
                quer = quer + " {} = %s,".format( simpleUpdateMap[attr] )
                params.append( getattr(w, attr) )
        
        quer = quer.rstrip(",") + " WHERE id = %s"
        params.append( w.id )
        
        try:
            cur = self.dbh.cursor()
            cur.execute(quer, params )
            if self.autoCommit:
                self.dbh.commit()
        except Exception as e:
            if self.autoCommit:
                self.dbh.rollback()
            raise e from None
        finally:
            cur.close()
