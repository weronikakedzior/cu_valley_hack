import csv
import os
import zipfile
from typing import List

from src.data import Sample
from src.database import InfluxDB
from src.parser import parse_csv


def save_samples(
    influx_client: InfluxDB,
    samples: List[Sample]
):
    for sample in samples:
        parsed_sample = influx_client.parse_sample(sample)
        influx_client.save_data(parsed_sample)


if __name__ == '__main__':

    data_path = './data'
    host = 'localhost'
    port = '8086'
    database = 'machines'

    influx_client = InfluxDB(
        host=host,
        port=port,
        database=database
    )

    influx_client.configure_database()

    # iterate over machines
    for machine in os.listdir(data_path):
        # read only WOS machines
        if not machine.startswith('WOS'):
            continue
        # iterate over years
        for year in os.listdir(
            os.path.join(data_path, machine)
        ):
            # iterate over months
            for month in os.listdir(
                os.path.join(data_path, machine, year)
            ):
                # iterate over days
                for day_zipfile in os.listdir(
                    os.path.join(data_path, machine, year, month)
                ):
                    # read only zip files
                    if not day_zipfile.endswith('.zip'):
                        continue

                    zip_path = os.path.join(
                        data_path, machine, year, month, day_zipfile
                    )

                    with zipfile.ZipFile(zip_path, 'r') as f:
                        first = True
                        for name in f.namelist():
                            data = f.read(name)
                            data = data.decode('utf-8').splitlines()
                            samples_list = parse_csv(
                                data=data,
                                csv_path=name
                            )
                            save_samples(influx_client, samples_list)
                            # print(samples_list)
