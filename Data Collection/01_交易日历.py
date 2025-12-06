import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import numpy as np
import tushare as ts
import glob

# -------------------------- Tushare 配置 (保持一致) --------------------------
# !!! 注意: 请替换为您的真实 Tushare Token !!!
TS_TOKEN = "374f3850d22ecc9f328be05c8f655f274ed5ff0c2c8a45f39fd76092" 
try:
    ts.set_token(TS_TOKEN)
    PRO_API = ts.pro_api()
except Exception:
    print("FATAL ERROR: Tushare Token 配置失败或未配置。请检查 TS_TOKEN 变量。")
    sys.exit(1)
# -----------------------------------------------------------------------------

# --- 基础配置与通用函数 (集成在脚本内部) ---
BASE_DIR = r"D:\2025"
TODAY_DATE = datetime.now()
TODAY_DATE_STR = TODAY_DATE.strftime('%Y%m%d')

LOG_DIR = os.path.join(BASE_DIR, 'logs', TODAY_DATE_STR)
os.makedirs(LOG_DIR, exist_ok=True) 

OUTPUT_SUBDIR = os.path.join('股票数据', '基础数据', '交易日历')
OUTPUT_BASE_DIR = os.path.join(BASE_DIR, 'raw', OUTPUT_SUBDIR)
os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

OUTPUT_FILENAME = 'trade_calendar_full.csv'
OUTPUT_PATH = os.path.join(OUTPUT_BASE_DIR, OUTPUT_FILENAME)

# 定义交易所的严格开业日期硬约束 (YYYYMMDD 整数格式)
EXCHANGE_OPEN_DATES = {
    'SSE': 19901219, # 上海证券交易所正式开业日期
    'SZSE': 19910703, # 深圳证券交易所正式开业日期
    'BSE': 20211115  # 北京证券交易所正式开市日期
}
MIN_START_DATE = datetime(1990, 1, 1).strftime('%Y%m%d')

def write_log(message, script_name):
    """写入运行日志文件。"""
    log_file = os.path.join(LOG_DIR, f"{script_name}_run.log")
    formatted_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(formatted_message)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(formatted_message + '\n')

def get_last_collected_date():
    """增量采集功能：获取已落地数据的最新日期。"""
    if not os.path.exists(OUTPUT_PATH):
        write_log("未发现历史文件，将从 19900101 开始全量采集。", '01_交易日历')
        return MIN_START_DATE
    
    try:
        # 只读取最后几行以加速
        df_old = pd.read_csv(OUTPUT_PATH, usecols=['cal_date'], dtype={'cal_date': str})
        latest_date = df_old['cal_date'].max()
        
        # 返回已采集的最新日期
        return latest_date
    except Exception as e:
        write_log(f"读取历史文件失败，将从 19900101 开始全量采集: {e}", '01_交易日历')
        return MIN_START_DATE

def save_to_csv(df, script_name, subdir):
    """数据落地函数，CSV 格式，支持历史文件合并和覆盖。"""
    
    try:
        # 严格遵守字段契约，只保留要求的四个字段
        df = df[['exchange', 'cal_date', 'is_open', 'pretrade_date']]
        
        # 尝试读取历史数据进行合并
        if os.path.exists(OUTPUT_PATH):
            df_old = pd.read_csv(OUTPUT_PATH, dtype={'cal_date': str, 'pretrade_date': str})
            # 合并新旧数据，并去重，保证 cal_date, exchange 唯一
            df_combined = pd.concat([df_old, df]).drop_duplicates(subset=['cal_date', 'exchange'], keep='last')
        else:
            df_combined = df.copy()

        # 按日期和交易所排序，最终覆盖写入
        df_combined = df_combined.sort_values(by=['cal_date', 'exchange']).reset_index(drop=True)
        
        df_combined.to_csv(OUTPUT_PATH, index=False, encoding='utf-8')
        write_log(f"数据成功落地到全局文件 (CSV): {OUTPUT_PATH}", script_name)
    except Exception as e:
        write_log(f"数据落地失败 (CSV)：{e}", script_name)
        sys.exit(1)

    write_log(f"--- {script_name} 执行成功，最终记录数: {len(df_combined)} ---", script_name)
# --- 基础配置与通用函数 END ---


def fetch_data_01_trade_calendar(start_date, end_date):
    """
    Tushare 真实 API 调用：获取全量交易日历（满足所有字段和约束）。
    """
    script_name = "01_交易日历"
    write_log(f"Tushare API 调用：获取 {start_date} 至 {end_date} 交易日历 (增量采集)...", script_name)

    exchanges = ['SSE', 'SZSE', 'BSE']
    df_list = []
    
    try:
        # 1. 以 SSE 为基准获取国家法定交易日历
        df_base = PRO_API.trade_cal(
            exchange='SSE',              
            start_date=start_date,
            # Tushare API 拉取的数据通常包含未来，我们拉到 today+1年 作为缓冲
            end_date=(TODAY_DATE + timedelta(days=365)).strftime('%Y%m%d'),
            is_open='',                   
            fields='cal_date,is_open,pretrade_date'
        )
        
        # 将 is_open 字段强制转为 int (0或1)
        df_base['is_open'] = pd.to_numeric(df_base['is_open'], errors='coerce').fillna(0).astype(int)
        df_base['cal_date_int'] = df_base['cal_date'].astype(int)

        # 2. 复制数据并应用严格的历史硬约束
        for ex in exchanges:
            df_ex = df_base.copy()
            df_ex['exchange'] = ex
            
            # 严格应用交易所成立日期的硬约束：
            cutoff_date_int = EXCHANGE_OPEN_DATES.get(ex)
            
            df_ex['is_open'] = np.where(
                df_ex['cal_date_int'] < cutoff_date_int,
                0, 
                df_ex['is_open']
            )
            
            df_list.append(df_ex)

        df = pd.concat(df_list, ignore_index=True)
        df = df.drop(columns=['cal_date_int']) # 清理辅助列
        df = df.sort_values(by=['cal_date', 'exchange']).reset_index(drop=True)
        
        # 转换 is_open 为字符串 (0或1) 以符合用户最初的字段类型要求
        df['is_open'] = df['is_open'].astype(str) 
        
    except Exception as e:
        write_log(f"Tushare API 调用失败：{e}", script_name)
        return pd.DataFrame() 
        
    write_log(f"API 调用成功：获取 {len(df)} 条记录 (合并 3 个交易所)。", script_name)
    return df

def run_01_trade_calendar():
    script_name = "01_交易日历"
    write_log(f"--- {script_name}.py 开始执行 ---", script_name)
    
    # 增量采集逻辑：从已采集的最新日期的后一天开始采集
    last_date = get_last_collected_date()
    start_date_dt = datetime.strptime(last_date, '%Y%m%d') + timedelta(days=1)
    
    # 如果上次采集的日期已经是今天或未来，则不需采集
    if start_date_dt.strftime('%Y%m%d') > (TODAY_DATE + timedelta(days=365)).strftime('%Y%m%d'):
         write_log("日历已包含未来数据，无需更新。", script_name)
         return
         
    start_date = start_date_dt.strftime('%Y%m%d')
    
    # 截止日期：拉取到今天+1年，保证日历的预测性
    end_date = (TODAY_DATE + timedelta(days=365)).strftime('%Y%m%d')
    
    df = fetch_data_01_trade_calendar(start_date, end_date)
    
    if df.empty:
        write_log("数据采集失败或返回空，硬停止！", script_name)
        sys.exit(1)
        
    # 落地到 CSV 格式，并与历史数据合并
    save_to_csv(df, script_name, subdir=OUTPUT_SUBDIR)

if __name__ == '__main__':
    run_01_trade_calendar()