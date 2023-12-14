import logging.config

from flask import Flask
from flask_cors import CORS
from flask_caching import Cache

from opentelemetry.instrumentation.flask import FlaskInstrumentor
from opentelemetry import trace    
from opentelemetry.sdk.resources import SERVICE_NAME as telemetery_service_name_key, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter

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
    'handlers': {
        'wsgi': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://flask.logging.wsgi_errors_stream',
            'formatter': 'default'
            },
        'file': {
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'default',
            'filename': 'cohd.log',
            'maxBytes': 10485760,
            'backupCount': 50
        }
    },
    'root': {
        'level': 'INFO',
        'handlers': ['wsgi', 'file']
    }
})

logging.info('About to instrument app for OTEL')
# set the service name for our trace provider 
# this will tag every trace with the service name given
tp = TracerProvider(
        resource=Resource.create({telemetery_service_name_key: 'COHD'})
    )
# create an exporter to jaeger     
jaeger_host = 'localhost'
deployment_env = app.config.get('DEPLOYMENT_ENV', 'dev')
if deployment_env[:4] == 'ITRB':
    jaeger_host = 'jaeger-otel-agent.sri'
jaeger_exporter = JaegerExporter(
            agent_host_name=jaeger_host,
            agent_port=6831,
        )
# here we use the exporter to export each span in a trace
tp.add_span_processor(
    BatchSpanProcessor(jaeger_exporter)
)
trace.set_tracer_provider(
    tp
)
otel_excluded_urls = 'health,api/health'
tracer = trace.get_tracer(__name__)
FlaskInstrumentor().instrument_app(app,
                                   excluded_urls=otel_excluded_urls)
logging.info('Finished instrumenting app for OTEL')