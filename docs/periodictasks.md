In a cluster periodic tasks
* are executed by one thread at a time. 
* never are two threads at the same time doing the same periodic task
* have a certain schedule
  * starting timestamp
  * timeinterval (milliseconds, seconds, minutes, hours, days, weeks, months, years)
  * calendar (weekday, dayofmonth, )
* have a state per node
  * UNHANDLED: nobody seems to handle the task currently
    * nobody announced having claimed, claiming or handling a task in the last recogtime
    * recognized DROPPING for the task and no other announcements
    * waits random time between 0 and claimtime for (CLAIMING, CLAIMED, HANDLING) before self announcing CLAIMING
  * CLAIMED: one known thread seems to handle the schedule
    * will handle the task the next schedule. 
    * announces having claimed the task 1 times per recogtime/2
  * CLAIMING: at least one thread tries to claim a task
    * announces intention to claim a task
    * looks if other threads already told CLAIMING
    * the first CLAIMING will claim and handle, all others will set to CLAIMED  
  * DROPPING: the thread previously having CLAIMED a task drops it.
    * does not schedule the task anymore
  * HANDLING: one known thread (host, process, thread) currently handles a task
    * has started HANDLING a task
    * keeps informing that currently HANDLING a task periodically (recogtime / 2)
    * informs about having CLAIMED the task after completion
* a thread having claimed a task announces that periodically 1 x minute