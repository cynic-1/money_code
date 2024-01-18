from pandas import DataFrame


def list2df_kline(data: list):
    df = DataFrame(data)
    df.columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'turnover']
    df.drop(['high', 'low', 'close', 'volume', 'close_time', 'turnover'], axis=1, inplace=True)
    df.set_index('open_time', inplace=True)
    df.sort_index(inplace=True)
    df['open'] = df['open'].astype('float64')
    return df


def pair2token(data: list):
    token_list = []
    for token in data:
        if token.endswith('USDT') or token.endswith('USDC'):
            token_list.append(token[:-4])
        elif token.endswith('BTC') or token.endswith('ETH'):
            token_list.append(token[:-3])
    unique_list = list(set(token_list))
    unique_list.sort()
    return unique_list
