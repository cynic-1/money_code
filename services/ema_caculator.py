from pandas import DataFrame
from utils.logger import Logger
import pandas as pd

class EmaCaculator:
    def __init__(self, exchangeAPI):
        self.exchangeAPI = exchangeAPI
        self.ema_list = [20, 200]
        self.btc = self.exchangeAPI.init_history_price('BTC')
        # 添加BTC数据检查
        self._validate_btc_data()

    def _validate_btc_data(self):
        """验证BTC数据的完整性"""
        if self.btc is None or self.btc.empty:
            raise ValueError("BTC historical data is empty")
        
        # 检查是否有空值或0值
        zero_or_null = self.btc['close'].isin([0, None, float('nan')])
        if zero_or_null.any():
            problematic_indices = self.btc[zero_or_null].index
            raise ValueError(f"BTC data contains zero or null values at indices: {problematic_indices}")

    def calculate_all_ema(self, prices: DataFrame) -> DataFrame:
        try:
            # 确保时间戳对齐
            if not all(ts in self.btc.index for ts in prices.index):
                missing_ts = [ts for ts in prices.index if ts not in self.btc.index]
                raise ValueError(f"Missing BTC data for timestamps: {missing_ts}")

            # 检查除数是否为0或空
            btc_close = self.btc.loc[prices.index, 'close']
            if (btc_close == 0).any() or btc_close.isnull().any():
                problematic_ts = btc_close[btc_close.isin([0, None]) | btc_close.isnull()].index
                raise ValueError(f"Invalid BTC prices at timestamps: {problematic_ts}")

            # 计算vbtc并验证结果
            prices['vbtc'] = prices['close'] / btc_close
            
            # 验证计算结果
            if prices['vbtc'].isnull().any():
                null_indices = prices[prices['vbtc'].isnull()].index
                raise ValueError(f"Calculated vbtc contains null values at indices: {null_indices}")

            # 计算EMA
            for num in self.ema_list:
                prices[f'btc_ema_{num}'] = prices['vbtc'].ewm(span=num, adjust=False).mean()
                prices[f'usd_ema_{num}'] = prices['close'].ewm(span=num, adjust=False).mean()

            return prices

        except Exception as e:
            Logger.get_logger().error(f"Error in calculate_all_ema: {str(e)}")
            # 可以选择重新抛出异常或返回适当的默认值
            raise

    def update_ema(self, prices: DataFrame, ema) -> DataFrame:
        try:
            ema_list = {}
            _, ema_list['usd_ema_20'], ema_list['usd_ema_200'], ema_list['btc_ema_20'], ema_list['btc_ema_200'], count = ema
            
            # 确保时间戳对齐
            if not all(ts in self.btc.index for ts in prices.index):
                missing_ts = [ts for ts in prices.index if ts not in self.btc.index]
                raise ValueError(f"Missing BTC data for timestamps: {missing_ts}")

            # 检查除数是否为0或空
            btc_close = self.btc.loc[prices.index, 'close']
            if (btc_close == 0).any() or btc_close.isnull().any():
                problematic_ts = btc_close[btc_close.isin([0, None]) | btc_close.isnull()].index
                raise ValueError(f"Invalid BTC prices at timestamps: {problematic_ts}")

            # 计算vbtc并验证结果
            prices['vbtc'] = prices['close'] / btc_close
            
            if prices['vbtc'].isnull().any():
                null_indices = prices[prices['vbtc'].isnull()].index
                raise ValueError(f"Calculated vbtc contains null values at indices: {null_indices}")

            for num in self.ema_list:
                prices[f'usd_ema_{num}'] = _update_ema(float(ema_list[f'usd_ema_{num}']), prices['close'], num)
                prices[f'btc_ema_{num}'] = _update_ema(float(ema_list[f'btc_ema_{num}']), prices['vbtc'], num)

            return prices

        except Exception as e:
            Logger.get_logger().error(f"Error in update_ema: {str(e)}")
            raise

def _update_ema(last_ema, new_data, span):
    try:
        multiplier = 2 / (span + 1)
        ema = [last_ema]

        for price in new_data:
            if pd.isnull(price) or price is None:
                raise ValueError(f"Invalid price value: {price}")
            ema.append((price - ema[-1]) * multiplier + ema[-1])

        return ema[1:]
    except Exception as e:
        Logger.get_logger().error(f"Error in _update_ema: {str(e)}")
        raise