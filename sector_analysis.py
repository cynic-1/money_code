from dataclasses import dataclass
from datetime import datetime
from typing import List, Dict, Optional
from collections import defaultdict
import pandas as pd
import psycopg2

@dataclass(frozen=True)
class CrossSignal:
    symbol: str
    cross_time: datetime
    cross_type: str
    rank: Optional[int]

class SectorAnalyzer:
    def __init__(self, db_params: Dict):
        self.db_params = db_params
        self.sector_data = defaultdict(list)
        self.latest_signals = {}
        
    def load_data(self, tags_csv: str):
        # 加载标签数据
        tags_df = pd.read_csv(tags_csv)
        tags_df['Tags'] = tags_df['Tags'].fillna('').str.split(',')
        symbol_tags = {
            row['symbol']: [tag.strip() for tag in row['Tags'] if tag.strip()] 
            for _, row in tags_df.iterrows()
        }
        symbol_rank = dict(zip(tags_df['symbol'], tags_df['rank']))
        
        # 从数据库加载交叉信号
        query = """
        SELECT symbol, cross_time, cross_type
        FROM ema_cross_view 
        ORDER BY cross_time DESC
        """
        
        with psycopg2.connect(**self.db_params) as conn:
            cross_df = pd.read_sql(query, conn, parse_dates=['cross_time'])  # 确保解析日期
            
        # 处理每个交叉信号
        for _, row in cross_df.iterrows():
            symbol = row['symbol']
            
            # 跳过没有标签的代币
            if not symbol_tags.get(symbol):
                continue
                
            signal = CrossSignal(
                symbol=symbol,
                cross_time=row['cross_time'],
                cross_type=row['cross_type'],
                rank=symbol_rank.get(symbol)
            )
            
            # 更新最新信号
            if symbol not in self.latest_signals or signal.cross_time > self.latest_signals[symbol].cross_time:
                self.latest_signals[symbol] = signal
            
            # 将信号添加到对应的板块
            for tag in symbol_tags[symbol]:
                self.sector_data[tag].append(signal)
    
    def analyze_sectors(self):
        results = []
        
        for sector, signals in self.sector_data.items():
            # 获取板块中所有唯一的代币
            unique_symbols = len(set(signal.symbol for signal in signals))
            
            # 跳过代币数量小于4的板块
            if unique_symbols < 4:
                continue
            
            # 获取每个代币的最新状态
            current_golden = sum(1 for symbol in set(s.symbol for s in signals)
                               if self.latest_signals[symbol].cross_type == "golden")
            current_death = unique_symbols - current_golden
            
            # 创建一个字典来存储每个symbol的最新信号
            latest_symbol_signals = {}
            for signal in signals:
                if (signal.symbol not in latest_symbol_signals or 
                    signal.cross_time > latest_symbol_signals[signal.symbol].cross_time):
                    latest_symbol_signals[signal.symbol] = signal
            
            # 转换为列表并排序
            sorted_signals = sorted(
                latest_symbol_signals.values(),
                key=lambda x: x.cross_time,
                reverse=True
            )
            
            results.append({
                "sector": sector,
                "stats": {
                    "total_symbols": unique_symbols,
                    "current_golden": current_golden,
                    "current_death": current_death
                },
                "signals": sorted_signals  # 直接存储CrossSignal对象
            })
        
        return sorted(results, key=lambda x: x['stats']['total_symbols'], reverse=True)
    
    def print_analysis(self):
        results = self.analyze_sectors()
        
        for sector_data in results:
            sector = sector_data["sector"]
            stats = sector_data["stats"]
            signals = sector_data["signals"]
            
            print(f"\n{'='*50}")
            print(f"板块: {sector}")
            print(f"代币总数: {stats['total_symbols']}")
            print(f"当前金叉数量: {stats['current_golden']}")
            print(f"当前死叉数量: {stats['current_death']}")
            print("\n最近信号:")
            print(f"{'Symbol':<10} {'信号类型':<8} {'时间':<20} {'排名':<8}")
            print('-' * 50)
            
            for signal in signals:
                cross_type = "金叉" if signal.cross_type == "golden" else "死叉"
                time_str = signal.cross_time.strftime('%Y-%m-%d %H:%M')
                rank = f"#{signal.rank}" if signal.rank else "N/A"
                print(f"{signal.symbol:<10} {cross_type:<8} {time_str:<20} {rank:<8}")

# 使用示例
if __name__ == "__main__":
    db_params = {
        "host": "localhost",
        "database": "cex",
        "user": "trader",
        "password": "trader_2024"
    }
    
    analyzer = SectorAnalyzer(db_params)
    analyzer.load_data("token_tags.csv")
    analyzer.print_analysis()