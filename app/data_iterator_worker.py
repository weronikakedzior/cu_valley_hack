import os
import zipfile
from datetime import datetime
from typing import List
import time

from tqdm import tqdm

# from src.data import Sample
# from src.database import InfluxDB
# from src.parser import parse_csv
from celery_base import app
from logger import get_logger
from parser_worker import parse_day

logger = get_logger('data-iterator-worker')
logger.propagate = False

N_DAYS_PER_MACHINE = 90


@app.on_after_configure.connect
def data_iterator(sender, **kwargs):

    data_path = os.environ['data_path']
    print('kupaaaaaaaaaaaaaaaaaaaa')
    print(len(os.listdir(data_path)))
    print('kuuuuuuuuuuuuuuuuuuuupa')
    # iterate over machines
    machines = sorted(
        os.listdir(data_path),
        reverse=True
    )
    for machine in machines:
        # read only WOS machines
        if not machine.startswith('WOS'):
            continue
        # print('=='*20)
        counter = 0
        # iterate over years
        years = sorted(
            os.listdir(os.path.join(data_path, machine)),
            reverse=True
        )
        for year in years:
            # read only 20** dirs
            if not year.startswith('20'):
                continue
            # iterate over months
            months = sorted(
                os.listdir(os.path.join(data_path, machine, year)), 
                reverse=True
            )
            for month in months:
                if len(month) != 2:
                    continue
                # iterate over days
                days = sorted(
                    os.listdir(os.path.join(data_path, machine, year, month)), 
                    key=lambda x: x.split('.zip')[0][-2:]
                )[::-1]
                for day_zipfile in days:
                    if not day_zipfile.endswith('.zip'):
                        continue
                    counter += 1
                    if counter <= N_DAYS_PER_MACHINE:
                        print(machine, year, month, day_zipfile)
                        parse_day.delay(
                            zip_path=os.path.join(
                                data_path, machine, year, month, day_zipfile
                            ),
                            machine_name=machine
                        )
