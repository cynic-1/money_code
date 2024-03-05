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


def gate_list2df_kline(data: list):
    df = DataFrame(data)
    df.columns = ['timestamp', 'turnover', 'close', 'high', 'low', 'open', 'volume', 'is_over']
    df.drop(['volume', 'is_over', 'turnover'], axis=1, inplace=True)
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
    seen = set()  # 创建一个空集合用来存储已经看到的元素
    for symbol_map in data:
        # 创建一个元组，包含我们想要去重的字段
        identifier = (symbol_map['baseAsset'], symbol_map['fullName'])

        # 检查这个元组是否已经在集合中
        if identifier not in seen:
            res.append(
                {
                    'symbol': symbol_map['baseAsset'],
                    'full_name': symbol_map['fullName']
                }
            )
            seen.add(identifier)  # 将这个元组加入到集合中，以便记录这个元素已经见过

    return res

def gate_list2symbol_fullname(data: list):
    res = []
    seen = set()  # 创建一个空集合用来存储已经看到的元素
    for symbol_map in data:
        # 创建一个元组，包含我们想要去重的字段
        identifier = (symbol_map['symbol'], symbol_map['name'])

        # 检查这个元组是否已经在集合中
        if identifier not in seen:
            res.append(
                {
                    'symbol': symbol_map['symbol'],
                    'full_name': symbol_map['name']
                }
            )
            seen.add(identifier)  # 将这个元组加入到集合中，以便记录这个元素已经见过

    return res
