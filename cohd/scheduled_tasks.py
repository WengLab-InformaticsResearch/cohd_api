# import atexit
from apscheduler.schedulers.background import BackgroundScheduler
import logging

from .biolink_mapper import BiolinkConceptMapper
from .app import app


def task_build_cache():
    print('Running scheduled task to build cache')
    BiolinkConceptMapper.build_mappings()


# Schedule a task to build the cache every first Saturday of the month (in ITRB-CI and Dev only)
deployment_env = app.config.get('DEPLOYMENT_ENV', 'dev').lower()
if deployment_env in ('itrb-ci', 'dev'):
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=task_build_cache, trigger='cron', day='1st sat', hour=0)
    scheduler.start()
    logging.info(f'Background task scheduled to build Biolink mappings (env: {deployment_env})')
else:
    logging.info(f'Background task NOT scheduled to build Biolink mappings (env: {deployment_env})')


# Registering a shutdown seems to cause UWSGI to have issues shutting down COHD, even with the wait=False option.
# Since the default job store does not persist jobs anyway, remove this line for now
# atexit.register(lambda: scheduler.shutdown(wait=False))
