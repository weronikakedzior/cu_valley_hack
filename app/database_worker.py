import os
from typing import List

from celery_base import app
from data import Sample
from database import InfluxDB
from logger import get_logger


@app.task(bind=True, name='database_insert')
# serializer='pickle', queue='database_queue')
def database_insert(
    samples: List[Sample]
):

    influx_client = InfluxDB(
        host=os.environ['influx_host'],
        port=os.environ['influx_port'],
        database=os.environ['influx_database']
    )

    influx_client.configure_database()

    logger = get_logger('database-worker')
    logger.info(
        'Inserting to database ', str(len(samples)), ' samples.'
    )

    influx_client.save_samples(samples)
