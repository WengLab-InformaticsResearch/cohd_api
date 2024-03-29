import logging.config

from flask import Flask
from flask_cors import CORS
from flask_caching import Cache

try:
    from opentelemetry.instrumentation.flask import FlaskInstrumentor
    from opentelemetry import trace
    from opentelemetry.sdk.resources import SERVICE_NAME as telemetery_service_name_key, Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.jaeger.thrift import JaegerExporter
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
    from opentelemetry.instrumentation.pymysql import PyMySQLInstrumentor
    OTEL_ENABLED = True
except ImportError:
    OTEL_ENABLED = False

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

if OTEL_ENABLED:
    logging.info('About to instrument app for OTEL')
    # set the service name for our trace provider
    # this will tag every trace with the service name given
    otel_service_name = app.config.get('OTEL_SERVICE_NAME', 'COHD')
    tp = TracerProvider(
            resource=Resource.create({telemetery_service_name_key: otel_service_name})
        )
    # create an exporter to jaeger
    jaeger_host = app.config.get('JAEGER_HOST', 'jaeger-otel-agent.sri')
    jaeger_port = app.config.get('JAEGER_PORT', 6831)
    deployment_env = app.config.get('DEPLOYMENT_ENV', 'dev')
    jaeger_exporter = JaegerExporter(
                agent_host_name=jaeger_host,
                agent_port=jaeger_port,
            )
    # here we use the exporter to export each span in a trace
    tp.add_span_processor(
        BatchSpanProcessor(jaeger_exporter)
    )
    trace.set_tracer_provider(
        tp
    )
    otel_excluded_urls = 'health,api/health,api/dev/.*'
    tracer = trace.get_tracer(__name__)
    FlaskInstrumentor().instrument_app(app,
                                       excluded_urls=otel_excluded_urls)
    RequestsInstrumentor().instrument()
    PyMySQLInstrumentor().instrument()
    logging.info('Finished instrumenting app for OTEL')
