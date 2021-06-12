from dataclasses import asdict
from typing import List

from influxdb import InfluxDBClient

from data import Sample


class InfluxDB:
    def __init__(self, host, port, database):
        self.host = host
        self.port = port
        self.database = database
        self.client = InfluxDBClient(
            host=self.host,
            port=self.port,
            database=self.database
        )

    def configure_database(self):
        # self.client.drop_database(self.database)
        self.client.create_database(self.database)
        # self.client.switch_database(self.database)

    def save_samples(self, samples: List[Sample]):
        parsed_samples = []
        for sample in samples:
            parsed_samples.append(self._parse_sample(sample))
        self.client.write_points(parsed_samples)

    def _parse_sample(self, sample: Sample):
        sample_as_dict = asdict(sample)
        del sample_as_dict['time']
        del sample_as_dict['machine_name']
        return {
                "measurement": sample.machine_name,
                "time": sample.time,
                "fields": sample_as_dict
            }
