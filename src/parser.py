# import pandas as pd
import csv
from datetime import datetime
from typing import List

from .data import Sample, WOSSample


def parse_csv(
    data: List[str],
    csv_path: str,
    machine_name: str = None
) -> List[Sample]:
    if not machine_name:
        machine_name = csv_path.split('_', 1)[1].split('_20')[0]

    sample_list = []

    # save all lines ending with '192' except the first
    save_row = False

    samples_dict = {}
    prev_msrt_datetime = datetime.strptime(
        '1997/09/10_15:03:00', 
        '%Y/%m/%d_%H:%M:%S'
    )

    for row in data:
        row = row.split('|')
        if row[-1] == '192':
            if save_row:
                msrt_tag = row[0].split(machine_name)[1].split('_')[0]
                msrt_date = row[2]
                msrt_time = row[3].split('.')[0]
                msrt_value = row[-2]

                msrt_datetime = msrt_date + '_' + msrt_time
                msrt_datetime = datetime.strptime(
                    msrt_datetime, 
                    '%Y/%m/%d_%H:%M:%S'
                )

                if prev_msrt_datetime != msrt_datetime:
                    samples_dict[msrt_datetime] = {}
                
                samples_dict[msrt_datetime][]
                

            save_row = True
    
    samples_list = []
    for key, value in samples_dict.items():
        samples_list.append(WOSSample(**value))

    return sample_list
