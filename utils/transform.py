from pandas import DataFrame

def list2df_kline(data: list):
    df = DataFrame(data)
    df.columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'turnover']
    df.drop(['high', 'low', 'close', 'volume', 'close_time', 'turnover'], axis=1, inplace=True)
    df.set_index('open_time', inplace=True)
    df.sort_index(inplace=True)
    df['open'] = df['open'].astype('float64')
    return df