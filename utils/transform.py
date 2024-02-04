from pandas import DataFrame


def list2df_kline(data: list):
    df = DataFrame(data)
    df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'turnover']
    df.drop(['volume', 'close_time', 'turnover'], axis=1, inplace=True)
    df.set_index('timestamp', inplace=True)
    df.sort_index(inplace=True)
    df['open'] = df['open'].astype('float64')
    df['high'] = df['high'].astype('float64')
    df['low'] = df['low'].astype('float64')
    df['close'] = df['close'].astype('float64')
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


def list2symbol_fullname(data: list):
    res = []
    for symbol_map in data:
        res.append(
            {
                'symbol': symbol_map['baseAsset'],
                'full_name': symbol_map['fullName']
            }
        )
    return res
