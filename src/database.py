from dataclasses import asdict

from influxdb import InfluxDBClient

from .data import WOSSample


class InfluxDB:
    def __init__(self, host, port, database):
        self.host = host
        self.port = port
        self.database = database
        self.client = InfluxDBClient(host=self.host, port=self.port, database=self.database)

    def configure_database(self):
        self.client.create_database(self.database)

    def parse_sample(self, sample: WOSSample):
        sample_as_dict = asdict(sample)
        del sample_as_dict['time']
        del sample_as_dict['machine_name']
        return [
            {
                "measurement": sample.machine_name,
                "time": sample.time,
                "fields": sample_as_dict
            }
        ]

    def save_data(self, parsed_data):
        self.client.write_points(parsed_data)
