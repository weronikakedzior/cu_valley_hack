import os
from celery import Celery


# prepare env CELERY_BROKER_URL
os.environ['CELERY_BROKER_URL'] = ''.join([
    os.environ['broker_protocol'],
    '://',
    os.environ['broker_user'],
    ':',
    os.environ['broker_password'],
    '@',
    os.environ['broker_host'],
    ':',
    os.environ['broker_port']
])

# prepare celery app
app = Celery()
app.conf.update({
    'task_routes': {
        'parse_day': {'queue': 'parser_queue'},
        'database_insert': {'queue': 'database_queue'}
    },
    'task_serializer': 'pickle',
    'result_serializer': 'pickle',
    'accept_content': ['pickle']
})
