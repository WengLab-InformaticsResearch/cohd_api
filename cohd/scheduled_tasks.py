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

# Registering a shutdown seems to cause UWSGI to have issues shutting down COHD, even with the wait=False option.
# Since the default job store does not persist jobs anyway, remove this line for now
# atexit.register(lambda: scheduler.shutdown(wait=False))
