import atexit
from apscheduler.schedulers.background import BackgroundScheduler

from .cohd_trapi import BiolinkConceptMapper


def task_build_cache():
    print('Running scheduled task to build cache')
    BiolinkConceptMapper.build_cache_map_from()


# Schedule a task to build the cache every first Saturday of the month
scheduler = BackgroundScheduler()
scheduler.add_job(func=task_build_cache, trigger='cron', day='1st sat', hour=4)
scheduler.start()
atexit.register(lambda: scheduler.shutdown())
