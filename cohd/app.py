import logging.config

from flask import Flask, request, redirect
from flask_cors import CORS
from flask_caching import Cache

from .google_analytics import GoogleAnalytics

#########
# INITS #
#########

app = Flask(__name__)
CORS(app)
app.config.from_pyfile('cohd_flask.conf')
cache = Cache(app)

# Logging config for logfile (not TRAPI log) (see: https://flask.palletsprojects.com/en/1.1.x/logging/)
logging.config.dictConfig({
    'version': 1,
    'formatters': {'default': {
        'format': '[%(asctime)s] %(levelname)s in %(module)s thread%(thread)d: %(message)s',
    }},
    'handlers': {'wsgi': {
        'class': 'logging.StreamHandler',
        'stream': 'ext://flask.logging.wsgi_errors_stream',
        'formatter': 'default'
    }},
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi']
    }
})
