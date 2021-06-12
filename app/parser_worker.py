import zipfile
from celery import signature
# from datetime import datetime

from celery_base import app
# from .data import WOSSample
from database_worker import database_insert
from logger import get_logger
from parser import parse_csv


@app.task(bind=True, name='parse_day')  #, queue="parser", serializer='pickle')
def parse_day(
    zip_path: str,
    machine_name: str
):
    logger = get_logger('parser-worker')
    logger.info('Starting parsing day for machine ', machine_name)

    with zipfile.ZipFile(zip_path, 'r') as f:
        samples_list = []
        for name in f.namelist():
            data = f.read(name)
            data = data.decode('utf-8').splitlines()
            samples_list.extend(
                parse_csv(
                    data=data,
                    csv_path=name
                )
            )

    # database_insert.delay(samples_list)
    signature(
        'database_insert',
        args=(
            samples_list,
        )
    ).apply_async()
