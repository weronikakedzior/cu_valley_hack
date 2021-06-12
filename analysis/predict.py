import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from alibi_detect.cd import KSDrift
from influxdb import InfluxDBClient
from sklearn.metrics.pairwise import euclidean_distances
from tqdm import tqdm


def get_n_similar(
    timestamp,
    df,
    indices,
    n_similar: int = 10
):
    similarity_matrix = euclidean_distances(df.drop(columns=['ids']))

    idx = indices[timestamp]

    sim_scores = similarity_matrix[idx]

    df['sim_score'] = sim_scores

    # high_sim_score = df[
    #     df['sim_score'] < 10.0
    # ].shape[0]

    # percentage = high_sim_score / df.shape[0]
    # print(percentage)

    df['time'] = indices.index
    df = df.sort_values(
        ['sim_score'],
        ascending=True
    ).iloc[1:n_similar+1].reset_index()

    return df['time'].to_list()


def get_full_interval_data(
    date_start: datetime,
    machine_name: str,
    influx_client
):
    date_end = date_start + timedelta(hours=1)
    date_start_influx = date_start.replace(tzinfo=timezone.utc).timestamp() * 1000
    date_end_influx = date_end.replace(tzinfo=timezone.utc).timestamp() * 1000

    query_result = influx_client.query(
        f'SELECT median(*) FROM {machine_name} WHERE time >= {int(date_start_influx)}ms and time <= {int(date_end_influx)}ms GROUP BY time(1s) fill(null)',
        database='machines'
    ).get_points()

    return query_result


def difference(dataset, interval=1):
    diff = list()
    for i in range(interval, len(dataset)):
        value = dataset[i] - dataset[i - interval]
        diff.append(value)
    return diff


def get_ks_detector(reference_set, p_val=0.01):
    return KSDrift(
        np.array(difference(reference_set)),
        p_val=p_val
    )


def predict_drift(cd, val):
    prediction = cd.predict(
        np.array(difference(val)),
        drift_type='batch',
        return_p_val=True,
        return_distance=False
    )['data']
    return prediction['is_drift']


def predict(
    ref_row,
    df,
    indices,
    machine_name,
    influx_client,
    n_similar: int = 10
):
    ref_ts = ref_row.name

    df_copy = df.copy()
    indices_copy = indices.copy()

    df_copy = df_copy.append(ref_row, ignore_index=True)
    indices_copy[ref_ts] = len(df_copy) - 1

    similar_to_ref = get_n_similar(
        timestamp=ref_ts,
        df=df_copy,
        indices=indices_copy,
        n_similar=n_similar
    )

    ref_data = get_full_interval_data(
        date_start=ref_ts,
        machine_name=machine_name,
        influx_client=influx_client
    )

    ref_data_dict = defaultdict(list)
    for row in ref_data:
        for sensor, data in row.items():
            if data and sensor != 'time':
                ref_data_dict[sensor].append(float(data))

    predictions = defaultdict(int)

    for sim_ts in similar_to_ref:

        test_data = get_full_interval_data(
            date_start=sim_ts,
            machine_name=machine_name,
            influx_client=influx_client
        )

        test_data_dict = defaultdict(list)
        for row in test_data:
            for sensor, data in row.items():
                if data and sensor != 'time':
                    test_data_dict[sensor].append(float(data))

        for sensor, data in test_data_dict.items():
            if len(data) > 1:
                # print('Len data ', len(data))
                cd = get_ks_detector(data)
                if len(ref_data_dict[sensor]) > 1:
                    # print('Len ref data ', len(ref_data_dict[sensor]))
                    pred = predict_drift(cd, ref_data_dict[sensor])
                    if pred:
                        predictions[sensor] = predictions[sensor] + 1

    for sensor, votes in predictions.items():
        # print(votes)
        if votes > int(n_similar * 0.9):
            ref_row['pred_'+sensor] = 1
        else:
            ref_row['pred_'+sensor] = 0

    return ref_row


if __name__ == '__main__':

    tqdm.pandas()

    data_path = 'analysis/csv'
    n_similar = 10
    n_to_predict = 90
    n_for_test = 120

    influx_host = 'localhost'
    influx_port = 8086
    influx_database = 'machines'

    influx_client = InfluxDBClient(
        host=influx_host,
        port=influx_port,
        database=influx_database
    )
    influx_client.switch_database(influx_database)

    for f in os.listdir(data_path):
    # for f in ['WOS___176L_norm.csv']:
        machine_name = f.split('_norm')[0]

        if not f.endswith('_norm.csv'):
            continue

        df = pd.read_csv(
            os.path.join(data_path, f),
            parse_dates=['time'],
            index_col='time'
        )

        # split df
        df_for_test = df.iloc[-(n_for_test+n_to_predict):-n_to_predict]
        df_to_predict = df.iloc[-n_to_predict:]

        df_for_test['ids'] = list(range(df_for_test.shape[0]))
        indices = df_for_test['ids']

        df_to_predict = df_to_predict.progress_apply(
            lambda row: predict(
                row,
                df_for_test,
                indices,
                machine_name,
                influx_client,
                n_similar
            ),
            axis=1
        )

        new_cols = []
        cols = df_to_predict.columns
        for col in cols:
            new_cols.append(col.replace('median_', ''))

        df_to_predict.columns = new_cols

        out_dir = 'predictions'
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)

        out_file = machine_name + '.csv'
        print(machine_name)
        df_to_predict.to_csv(
            os.path.join(out_dir, out_file)
        )
