import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import numpy as np
import tushare as ts
from tqdm import tqdm 
import glob

# -------------------------- Tushare 配置 --------------------------
# !!! 注意: 需与 01 脚本使用相同的真实 Tushare Token !!!
TS_TOKEN = "374f3850d22ecc9f328be05c8f655f274ed5ff0c2c8a45f39fd76092" # 请确保您已在此处填入有效的Token
try:
    ts.set_token(TS_TOKEN)
    PRO_API = ts.pro_api()
except Exception:
    print("FATAL ERROR: Tushare Token 配置失败或未配置。请检查 TS_TOKEN 变量。")
    sys.exit(1)
# ---------------------------------------------------------------------------------------

# --- 基础配置与通用函数 (集成在脚本内部) ---
BASE_DIR = r"D:\2025"
TODAY_DATE = datetime.now()
TODAY_DATE_STR = TODAY_DATE.strftime('%Y%m%d')
LOG_DIR = os.path.join(BASE_DIR, 'logs', TODAY_DATE_STR)
os.makedirs(LOG_DIR, exist_ok=True) 

# 目标目录结构
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '基础数据', '股票列表')
OUTPUT_DIR = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 交易所开业日期，用于确定历史回溯起点
EXCHANGE_OPEN_DATES = {
    'SSE': '19901219',
    'SZSE': '19910703',
    'BSE': '20211115'
}
MIN_START_DATE = min(EXCHANGE_OPEN_DATES.values()) # 19901219

# 最终需要输出的 17 个基础字段 + trade_date 字段 (总共 18 个字段契约)
BASE_FIELDS_CONTRACT = [
    'ts_code', 'symbol', 'name', 'area', 'industry', 'fullname', 'enname', 
    'cnspell', 'market', 'exchange', 'curr_type', 'list_status', 
    'list_date', 'delist_date', 'is_hs', 'act_name', 'act_ent_type'
]
FIELD_CONTRACT = BASE_FIELDS_CONTRACT + ['trade_date']

def write_log(message, script_name):
    """写入运行日志文件。"""
    log_file = os.path.join(LOG_DIR, f"{script_name}_run.log")
    formatted_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(formatted_message)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(formatted_message + '\n')

def get_last_collected_date(output_dir):
    """断点续采/增量采集功能：获取已落地数据的最新分区日期。"""
    partition_dirs = glob.glob(os.path.join(output_dir, "trade_date=*"))
    if not partition_dirs:
        # 首次运行：返回一个早于最小起始日期的日期，使采集从 MIN_START_DATE 开始
        return '19901218' 
    
    dates = [os.path.basename(d).split('=')[-1] for d in partition_dirs]
    latest_date = max(dates)
    
    if latest_date < MIN_START_DATE or latest_date > TODAY_DATE_STR:
        return '19901218' 

    return latest_date

def save_to_csv_partitioned(df_day, script_name, subdir):
    """数据落地函数，CSV 格式，按 trade_date 分区。"""
    date_str = df_day['trade_date'].iloc[0]
    output_base_dir = os.path.join(BASE_DIR, 'raw', subdir)
    
    partition_dir = os.path.join(output_base_dir, f"trade_date={date_str}")
    os.makedirs(partition_dir, exist_ok=True)
    
    output_path = os.path.join(partition_dir, 'data.csv')
    
    try:
        # 严格遵守字段契约和顺序
        df_day[FIELD_CONTRACT].to_csv(output_path, index=False, encoding='utf-8')
        return True
    except Exception as e:
        write_log(f"数据落地失败 (CSV) on {date_str}: {e}", script_name)
        return False


def fetch_and_generate_history():
    """Tushare 真实 API 调用：获取全市场股票历史状态和交易日历。"""
    script_name = "02_股票列表"
    write_log(f"Tushare API 调用：获取全市场股票历史状态 (包含 {len(BASE_FIELDS_CONTRACT)} 个基础字段)...", script_name)

    # --- 1. 获取所有股票的上市/退市/基础信息 ---
    # 确保拉取所有 17 个基础字段
    fields_to_pull = ','.join(BASE_FIELDS_CONTRACT)
    
    try:
        df_basic = PRO_API.stock_basic(
            exchange='', 
            list_status='L,D,P', 
            fields=fields_to_pull
        )
        
        # 填充 NaN 值
        FUTURE_DATE = '99991231'
        df_basic['delist_date'] = df_basic['delist_date'].fillna(FUTURE_DATE)
        df_basic['act_name'] = df_basic['act_name'].fillna('N/A')
        df_basic['act_ent_type'] = df_basic['act_ent_type'].fillna('N/A')
        # 确保只有有上市日期的股票才参与计算
        df_basic = df_basic[df_basic['list_date'].notna()].copy()
        
    except Exception as e:
        write_log(f"Tushare API (stock_basic) 调用失败：{e}", script_name)
        return pd.DataFrame(), pd.DataFrame() 

    # --- 2. 获取交易日历（作为时间轴基准）---
    start_date = MIN_START_DATE 
    end_date = TODAY_DATE_STR
    
    write_log(f"拉取 {start_date} 至 {end_date} 交易日历...", script_name)
    try:
        df_cal = PRO_API.trade_cal(
            exchange='SSE',              
            start_date=start_date,
            end_date=end_date,
            is_open='1',                   # 只拉取交易日
            fields='cal_date'
        ).rename(columns={'cal_date': 'trade_date'})
        
    except Exception as e:
        write_log(f"Tushare API (trade_cal) 调用失败：{e}", script_name)
        return pd.DataFrame(), pd.DataFrame()
        
    write_log(f"API 调用成功：获取 {len(df_basic)} 条股票记录和 {len(df_cal)} 个交易日。", script_name)
    return df_basic, df_cal


def run_02_stock_list():
    script_name = "02_股票列表"
    write_log(f"--- {script_name}.py 开始执行 ---", script_name)
    
    # 步骤 1: 获取历史基础数据和交易日历
    df_basic, df_cal = fetch_and_generate_history()
    if df_basic.empty or df_cal.empty:
        write_log("基础数据拉取失败，硬停止！", script_name)
        sys.exit(1)

    # 步骤 2: 确定断点续采的起始日期
    last_collected_date = get_last_collected_date(OUTPUT_DIR)
    
    # 从 last_collected_date 的下一天开始计算
    start_date_dt = datetime.strptime(last_collected_date, '%Y%m%d') + timedelta(days=1)
    start_date_str = start_date_dt.strftime('%Y%m%d')
    
    if start_date_str > TODAY_DATE_STR:
        write_log("已采集到最新日期，无需更新。", script_name)
        return
    
    write_log(f"断点续采/增量采集：从 {start_date_str} 开始计算历史快照。", script_name)
    
    # 过滤交易日历，只保留需要采集的日期
    trade_dates = df_cal[
        (df_cal['trade_date'] >= start_date_str) & 
        (df_cal['trade_date'] <= TODAY_DATE_STR)
    ]['trade_date'].tolist()

    if not trade_dates:
        write_log("没有需要采集的新交易日。", script_name)
        return
        
    # --- 3. 生成历史每日在市快照并分区落地 ---
    
    # 将日期和上市/退市日期转为 int 格式，便于高效比较
    df_basic['list_date_int'] = df_basic['list_date'].astype(int)
    df_basic['delist_date_int'] = df_basic['delist_date'].astype(int)
    
    write_log(f"开始生成并分区落地 {len(trade_dates)} 天的快照...", script_name)

    # 使用进度条封装交易日循环
    for date_str in tqdm(trade_dates, desc=f"分区落地 [{script_name}]"):
        current_date_int = int(date_str)
        
        # 核心逻辑：股票的上市日期 <= 当前日期 < 退市日期
        df_daily = df_basic[
            (df_basic['list_date_int'] <= current_date_int) &
            (df_basic['delist_date_int'] > current_date_int)
        ].copy()
        
        # 交易所开业日期的硬约束
        df_daily['open_date_int'] = df_daily['exchange'].apply(lambda x: int(EXCHANGE_OPEN_DATES.get(x)))
        df_daily = df_daily[df_daily['open_date_int'] <= current_date_int]
        df_daily = df_daily.drop(columns=['open_date_int', 'list_date_int', 'delist_date_int'])

        if not df_daily.empty:
            df_daily['trade_date'] = date_str
            # 执行落地
            save_to_csv_partitioned(df_daily, script_name, subdir=OUTPUT_BASE_SUBDIR)
            
    write_log(f"--- {script_name} 历史全量快照生成完成。---", script_name)

if __name__ == '__main__':
    run_02_stock_list()