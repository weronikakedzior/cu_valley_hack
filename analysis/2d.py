machine = 'WOS___177L'
start_date = datetime.strptime('2021-02-19T18:00:00Z', '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
end_date = datetime.strptime('2021-02-20T04:00:00Z', '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)

data = pd.read_csv(os.path.join(DATA_PATH, machine+'_norm.csv'), parse_dates=['time'])

selected_times = data[(data['time'] > start_date) & (data['time'] < end_date)]['time']

df = data.copy()
method = 'tsne'

df['color'] = df.apply(
    lambda row: '1' if row['time'] in list(selected_times) else '0',
    axis=1
)

X = df.drop(columns=['time'])

if method == 'tsne':
    X_embedded = TSNE(
        n_components=2,
        random_state=RANDOM_STATE
    ).fit_transform(X)
    X_embedded = pd.DataFrame(X_embedded)
    df['x'] = X_embedded[0]
    df['y'] = X_embedded[1]
elif method=='pca':
    pca = PCA(n_components=2).fit_transform(X.values)
    df['x'] = pca[:,0]
    df['y'] = pca[:,1]
fig = px.scatter(df, x='x', y='y', color='color', hover_name='time')
fig.show()