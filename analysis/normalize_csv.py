import os

import pandas as pd
from sklearn import preprocessing
from tqdm import tqdm


if __name__ == "__main__":

    data_path = 'analysis/csv'

    for f in tqdm(os.listdir(data_path)):
        if not f.endswith('.csv'):
            continue
        df = pd.read_csv(os.path.join(data_path, f))

        x = df.drop(columns=['time']).values  # returns a numpy array
        min_max_scaler = preprocessing.MinMaxScaler()
        x_scaled = min_max_scaler.fit_transform(x)
        df_scaled = pd.DataFrame(
            x_scaled,
            columns=df.drop(columns=['time']).columns
        )
        df_scaled['time'] = df['time']
        cols = list(df_scaled.columns)
        cols.pop()
        cols.insert(0, 'time')
        df_scaled = df_scaled[cols]

        df_scaled.to_csv(
            os.path.join(data_path, f.replace('.csv', '_norm.csv')),
            index=False
        )
