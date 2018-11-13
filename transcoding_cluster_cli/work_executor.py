import time
import subprocess
import traceback

from transcoding_cluster import task as taskModule

class WorkExecutor( object ):
    # TODO: extra thread for sending alive messages to the server
    
    def __init__(self, client, workerId ):
        self.client = client
        self.workerId = workerId
        self.task = None

        self.pollWaitTime = 500
    
    def logLine( self, data ):
        taskId = -1
        if self.task != None:
            taskId = self.task.id
        data = data.rstrip('\n')
        for line in data.splitlines():
            print( "worker '{}', task {}: {}".format( self.workerId, taskId, line) )
    
    def failCurrentTask( self ):
        if self.task != None:
            try:
                self.task.status = taskModule.TaskStatus.failed
                self.client.updateTask( self.task, ["status",] )
            except Exception as e:
                # marking task as failed is best effort
                self.logLine( traceback.format_exc() )
            finally:
                self.task = None
        
    
    def mainLoop(self):
        doApiRateLimit = False
        while True:
            try:
                if doApiRateLimit:
                    # TODO: implement pub/sub wake up
                    time.sleep( self.pollWaitTime )
                    doApiRateLimit = False
                
                self.task = self.client.requestTask( self.workerId, peek=False)

                # TODO: feature idea: tasks can set ENV and cwd
                p = subprocess.Popen(self.task.command, stdout=subprocess.PIPE, shell=True)
                while p.poll() is None:
                    l = p.stdout.readline() # blocks 
                    self.logLine( l.decode() )
                # consume rest of the output
                self.logLine( p.stdout.read().decode() )

                if p.returncode == 0:
                    self.task.status = taskModule.TaskStatus.done
                else:
                    self.task.status = taskModule.TaskStatus.failed
                self.task = self.client.updateTask( self.task, ["status",] )

                self.task = None
                
            except Exception as e:
                doApiRateLimit = True
                self.logLine( traceback.format_exc() )
                self.failCurrentTask()
            except (SystemExit, KeyboardInterrupt) as e:
                self.failCurrentTask()
                # here we do not want to swallow the exception, but propagate the clean shutdown
                raise e from None
