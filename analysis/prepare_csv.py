import os

import pandas as pd
from influxdb import InfluxDBClient
from tqdm import tqdm

if __name__ == '__main__':

    out_dir = 'analysis/csv/'

    influx_host = 'localhost'
    influx_port = 8086
    influx_database = 'machines'

    influx_client = InfluxDBClient(
        host=influx_host,
        port=influx_port,
        database=influx_database
    )
    influx_client.switch_database(influx_database)

    # fields = [
    #     'BREAKP', 'ENGCOOLT', 'ENGHOURS', 'ENGOILP',
    #     'ENGRPM', 'ENGTPS', 'FUELUS', 'GROILP',
    #     'GROILT', 'HYDOILP', 'HYDOILT', 'INTAKEP',
    #     'INTAKET', 'SELGEAR', 'SPEED', 'TEMPIN',
    #     'TRNAUT', 'TRNBPS', 'TRNLUP'
    # ]
    machine_names = [
        'WOS___174L', 'WOS___175L', 'WOS___176L',
        'WOS___177L', 'WOS___179L'
    ]

    stat = 'median'

    for machine_name in tqdm(machine_names):
        query_result = influx_client.query(
            f'SELECT median(*) FROM {machine_name} WHERE time >= 1591951340495ms and time <= now() GROUP BY time(1h) fill(null)',
            database=influx_database
        ).get_points()

        rows_dicts = []

        for row in query_result:
            if len(set(list(row.values()))) > 2:
                rows_dicts.append(
                    row
                )

        machine_df = pd.DataFrame(rows_dicts)
        machine_df = machine_df.dropna()

        new_cols = []
        cols = machine_df.columns
        for col in cols:
            new_cols.append(col.replace('median_', ''))

        machine_df.columns = new_cols

        file_name = machine_name + '.csv'
        machine_df.to_csv(os.path.join(out_dir, file_name), index=False)
