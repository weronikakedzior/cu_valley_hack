from influxdb import InfluxDBClient
from datetime import datetime, timedelta, timezone

def get_full_interval_data(date_start: datetime, machine_name: str):
    date_end = date_start + timedelta(hours=1)
    date_start_influx = date_start.replace(tzinfo=timezone.utc).timestamp() * 1000
    date_end_influx = date_end.replace(tzinfo=timezone.utc).timestamp() * 1000
    query_result = influx_client.query(
            f'SELECT median(*) FROM {machine_name} WHERE time >= {int(date_start_influx)}ms and time <= {int(date_end_influx)}ms GROUP BY time(1s) fill(null)',
            database='machines'
        ).get_points()
    return query_result

if __name__ == '__main__':

    influx_host = 'localhost'
    influx_port = 8086
    influx_database = 'machines'

    influx_client = InfluxDBClient(
        host=influx_host,
        port=influx_port,
        database=influx_database
    )
    influx_client.switch_database(influx_database)
    machine_names = [
        'WOS___174L', 'WOS___175L', 'WOS___176L',
        'WOS___177L', 'WOS___179L'
    ]
    query_result = get_full_interval_data(datetime.strptime('2021-03-03T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ'), 'WOS___179L')
    for row in query_result:
        print(row)