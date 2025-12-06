import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import numpy as np
from tqdm import tqdm 
import glob

# -------------------------- 基础配置 --------------------------
SCRIPT_NAME = "03_ST股票列表(计算)"
BASE_DIR = r"D:\2025"
TODAY_DATE = datetime.now()
TODAY_DATE_STR = TODAY_DATE.strftime('%Y%m%d')
LOG_DIR = os.path.join(BASE_DIR, 'logs', TODAY_DATE_STR)
os.makedirs(LOG_DIR, exist_ok=True) 

# 数据源目录
CALENDAR_PATH = os.path.join(BASE_DIR, 'raw', '股票数据', '基础数据', '交易日历', 'trade_calendar_full.csv')
DAILY_DATA_DIR = os.path.join(BASE_DIR, 'raw', '股票数据', '行情数据', '历史日线')
LIMIT_DATA_DIR = os.path.join(BASE_DIR, 'raw', '股票数据', '行情数据', '每日涨跌停价格')

# 目标输出目录：按 trade_date 分区
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '基础数据', 'ST股票列表')
OUTPUT_DIR = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

MIN_START_DATE = '19901219' 
# ---------------------------------------------------------------------------------------

def write_log(message):
    """写入运行日志文件。"""
    log_file = os.path.join(LOG_DIR, f"{SCRIPT_NAME}_run.log")
    formatted_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(formatted_message)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(formatted_message + '\n')

def fetch_trade_dates():
    """读取交易日历，获取全市场的交易日列表。"""
    if not os.path.exists(CALENDAR_PATH):
        write_log(f"FATAL ERROR: 交易日历文件不存在，请先运行 01_交易日历.py。")
        sys.exit(1)
    
    try:
        df_cal = pd.read_csv(CALENDAR_PATH, usecols=['cal_date', 'is_open', 'exchange'])
        df_trade_dates = df_cal[
            (df_cal['exchange'] == 'SSE') & 
            (pd.to_numeric(df_cal['is_open'], errors='coerce') == 1)
        ]['cal_date'].astype(str).tolist() 
        
        return sorted([d for d in df_trade_dates if d >= MIN_START_DATE])
        
    except Exception as e:
        write_log(f"读取或解析交易日历文件失败: {e}。")
        sys.exit(1)

def get_last_processed_date():
    """断点续传：获取已处理的最新分区日期。"""
    partition_dirs = glob.glob(os.path.join(OUTPUT_DIR, "trade_date=*"))
    
    if not partition_dirs:
        # 返回最早日期前一天，以便从 MIN_START_DATE 开始
        start_dt = datetime.strptime(MIN_START_DATE, '%Y%m%d') - timedelta(days=1)
        return start_dt.strftime('%Y%m%d')
    
    dates = [os.path.basename(d).split('=')[-1] for d in partition_dirs]
    latest_date = max(dates)
    
    # 确保返回的日期有效
    if latest_date < MIN_START_DATE or latest_date > TODAY_DATE_STR:
        return datetime.strptime(MIN_START_DATE, '%Y%m%d').strftime('%Y%m%d')

    return latest_date

def load_data_snapshot(data_dir, date_str, required_cols):
    """加载特定日期分区数据，如果文件不存在则返回空DF。"""
    file_path = os.path.join(data_dir, f"trade_date={date_str}", 'data.csv')
    
    if not os.path.exists(file_path):
        return pd.DataFrame()
        
    try:
        # 只需要 'ts_code' 和需要的列
        df = pd.read_csv(file_path, usecols=['ts_code'] + required_cols, dtype={'ts_code': str})
        return df
    except Exception as e:
        write_log(f"警告: 读取 {date_str} 的数据文件失败 ({data_dir}): {e}")
        return pd.DataFrame()

def save_to_csv_partitioned(df, date_str, subdir):
    """数据落地函数，CSV 格式，按 trade_date 分区。"""
    partition_dir = os.path.join(BASE_DIR, 'raw', subdir, f"trade_date={date_str}")
    os.makedirs(partition_dir, exist_ok=True)
    
    output_path = os.path.join(partition_dir, 'data.csv')
    
    try:
        if df.empty:
            return False
            
        # 确保 DataFrame 包含所有需要的字段再保存
        df[['ts_code', 'trade_date', 'is_st']].to_csv(output_path, index=False, encoding='utf-8')
        return True
    except Exception as e:
        write_log(f"数据落地失败 (CSV) on {date_str}: {e}")
        return False

def run_03_st_calc():
    write_log(f"--- {SCRIPT_NAME}.py 开始执行 (计算模式 - 无 API 依赖) ---")
    
    # 步骤 1: 获取交易日历
    all_trade_dates = fetch_trade_dates()
    
    # 步骤 2: 确定断点续传的起始日期
    last_processed_date = get_last_processed_date()
    
    # 从上次处理的日期+1天开始
    start_date_dt = datetime.strptime(last_processed_date, '%Y%m%d') + timedelta(days=1)
    start_date_str = start_date_dt.strftime('%Y%m%d')
    
    # 过滤交易日历，只保留需要处理的日期
    trade_dates_to_process = [d for d in all_trade_dates if d >= start_date_str and d <= TODAY_DATE_STR]

    write_log(f"断点续传/增量计算：从 {start_date_str} 开始计算 ST 状态。")
    write_log(f"待处理交易日数量: {len(trade_dates_to_process)}") 
    if not trade_dates_to_process:
        write_log("没有需要处理的新交易日。")
        return
        
    # --- 3. 循环交易日，加载数据，计算并落地 ---
    write_log(f"开始处理 {len(trade_dates_to_process)} 天的数据...")

    # 设置容忍度：5% 涨跌幅（0.05），容差 0.1%（0.001）
    ST_LIMIT_PERCENTAGE = 0.05
    TOLERANCE = 0.001 
    processed_count = 0
    
    for date_str in tqdm(trade_dates_to_process, desc=f"计算 [{SCRIPT_NAME}]"):
        
        # 3.1 加载所需数据
        df_daily = load_data_snapshot(DAILY_DATA_DIR, date_str, ['pre_close'])
        df_limit = load_data_snapshot(LIMIT_DATA_DIR, date_str, ['up_limit'])
        
        if df_daily.empty or df_limit.empty:
            # 可能是数据还未采集完整，跳过当日
            write_log(f"警告: 交易日 {date_str} 的数据依赖（04/05）不完整，跳过。")
            continue 
            
        # 3.2 数据合并
        df_merged = pd.merge(
            df_daily, 
            df_limit, 
            on='ts_code', 
            how='inner'
        )
        
        if df_merged.empty:
            continue
            
        # ************* 关键修复区域 *************
        # 3.2.5 确保 trade_date 字段存在于 DataFrame 中
        df_merged['trade_date'] = date_str 
        # ****************************************

        # 3.3 核心计算逻辑：计算涨停价与昨收价的百分比差值
        df_merged['pre_close'] = pd.to_numeric(df_merged['pre_close'], errors='coerce')
        df_merged['up_limit'] = pd.to_numeric(df_merged['up_limit'], errors='coerce')
        
        df_merged = df_merged[df_merged['pre_close'] > 0].copy()
        
        df_merged['limit_pct'] = (df_merged['up_limit'] / df_merged['pre_close']) - 1
        
        # 3.4 判断 ST 状态
        df_merged['is_st'] = np.where(
            np.isclose(df_merged['limit_pct'], ST_LIMIT_PERCENTAGE, atol=TOLERANCE), 
            1, 
            0 
        )

        df_st_list = df_merged[df_merged['is_st'] == 1].copy()
        
        # 3.5 落地数据
        if save_to_csv_partitioned(df_st_list, date_str, OUTPUT_BASE_SUBDIR):
            processed_count += 1
            
    write_log(f"--- {SCRIPT_NAME} 计算完成。总计更新 {processed_count} 个交易日的 ST 列表。---")

if __name__ == '__main__':
    run_03_st_calc()