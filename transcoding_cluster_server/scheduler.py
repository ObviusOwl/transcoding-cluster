from transcoding_cluster import errors
from transcoding_cluster import task

class Scheduler( object ):
    
    def __init__(self, db):
        self.db = db

        # make sure to fit all scheduling logic into SQL, since this
        # removes the need for the distributed (write) lock at API level 
        self.nextTaskQuery = """
        SELECT *
        FROM tasks AS allTasks
        WHERE
            status = {stat_sched} # schedulable tasks
            AND NOT EXISTS
            ( 
                # select all unmet dependencies for this task
                SELECT dep_id
                FROM task_dependencies
                    INNER JOIN tasks ON task_dependencies.dep_id = tasks.id
                WHERE
                    tasks.status != {stat_done} # task is not done  
                    AND task_dependencies.task_id = allTasks.id
            )
            AND ( affinity IS NULL OR affinity = %s )
        ORDER BY priority DESC
        LIMIT 1
        FOR UPDATE
        """.format( 
            stat_sched=task.TaskStatus.schedulable.value, 
            stat_done=task.TaskStatus.done.value 
        )
        
        self.workerRunningTasksQuery = """
        SELECT * FROM tasks
        WHERE 
            worker_id = %s
            AND status = {stat_sched}
        """.format( 
            stat_sched=task.TaskStatus.dispatched.value 
        )
    
    def __enter__(self):
        # TODO: lock
        pass
    
    def __exit__(self, exc_type, exc_value, traceback):
        # TODO: release lock
        pass
    
    def requireWorkerFree(self, workerId):
        with self.db:
            w = self.db.getWorker( workerId )
        if w == None:
            raise errors.SchedulerError( "Worker is not registered" )
        if w.drain == True:
            raise errors.SchedulerError( "Worker is draining" )

        with self.db:
            tasks = self.db.queryTasks( self.workerRunningTasksQuery, (workerId,) )
        if len( tasks ) >= 1:
            raise errors.SchedulerError( "Worker must be free. Cancel all running tasks before requesting a new task." )

    def getNextTask(self, workerId, peek=False):
        with self.db:
            try:
                self.db.autoCommit = False
                self.db.start_transaction( consistent_snapshot=True, isolation_level="READ COMMITTED", readonly=False )

                self.requireWorkerFree( workerId )
                tasks = self.db.queryTasks( self.nextTaskQuery, (workerId,) )
                assert len(tasks) <= 1

                if len(tasks) == 0:
                    self.db.dbh.rollback()
                    return None
                
                t = tasks[0]
                t.status = task.TaskStatus.dispatched
                t.workerId = workerId

                if peek:
                    self.db.dbh.rollback()
                    return t
                else:
                    self.db.updateTask( t, ("status","workerId") )
                    self.db.dbh.commit()
                    return t

            except Exception as e:
                self.db.dbh.rollback()
                raise e from None
            finally:
                self.db.autoCommit = True

        return None
        
        