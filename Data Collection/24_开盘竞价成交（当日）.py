import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import tushare as ts
from tqdm import tqdm 
import time
import glob
from typing import List

# -------------------------- T0: 元信息 (Meta Info) --------------------------
SCRIPT_NAME = "24_开盘竞价成交（当日）"
INTERFACE_NAME = "stk_auction"
PARTITION_KEY = 'trade_date' # T4: 按交易日期分区
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '打板专题数据', '开盘竞价成交（当日）') 
OUTPUT_FORMAT = 'csv' 

# 全局工程配置
BASE_DIR = r"D:\2025" 
TODAY_DATE_STR = datetime.now().strftime('%Y%m%d')
MAX_RETRY = 3 
# T7: 遵守频率：修正为 10次/分钟 (即 6秒/次)
API_SLEEP_SECONDS = 6.1
PAGE_SIZE = 8000 # 接口默认分页大小
# 修正为证券交易所成立日期，确保全量回溯
DEFAULT_START_DATE = "20110812" 

# 交易日历文件的位置，用于确定时间轴
CALENDAR_PATH = os.path.join(BASE_DIR, 'raw', '股票数据', '基础数据', '交易日历', 'trade_calendar_full.csv')


# -------------------------- Tushare 配置与初始化 --------------------------
# 请替换为您的 Tushare Token
TS_TOKEN = "374f3850d22ecc9f328be05c8f655f274ed5ff0c2c8a45f39fd76092" # <--- 请在此处替换为你的 Tushare Token
try:
    ts.set_token(TS_TOKEN)
    PRO_API = ts.pro_api(timeout=60)
except Exception as e:
    print(f"FATAL ERROR: Tushare Token 配置失败或未配置。详细错误: {e}")
    sys.exit(1)
# -------------------------------------------------------------------------

# T3: 字段契约 (Contract)
OUTPUT_FIELDS = [
    'ts_code',
    'trade_date',
    'vol',
    'price',
    'amount',
    'pre_close',
    'turnover_rate',
    'volume_ratio',
    'float_share'
]
PK_COLUMNS = ['ts_code', PARTITION_KEY] # 主键：股票代码 + 交易日期

# -------------------------- T6/T7/T11/T10: 核心工程函数 --------------------------

def setup_directories():
    """初始化输出目录。"""
    output_dir = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

OUTPUT_DIR = setup_directories()

def write_log(message, level="INFO"):
    """仅打印到控制台，不再写入日志文件。"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # 打印到控制台
    print(f"[{timestamp}] [{level}] {SCRIPT_NAME.split('_', 1)[1]}: {message}")

def fatal_exit(message):
    """记录致命错误并退出 (T11: 零缺失硬停止)。"""
    write_log(message, "FATAL")
    sys.exit(1) 

# -------------------------- T10: 幂等性/断点续采核心函数 --------------------------

def get_latest_completed_date_from_disk():
    """ 
    通过检查磁盘上的 CSV 文件（隐式状态），确定已完成采集的最新交易日期。
    """
    if not os.path.exists(OUTPUT_DIR):
        return None

    # 1. 查找所有分区目录
    all_partitions = [d for d in os.listdir(OUTPUT_DIR) if d.startswith(f"{PARTITION_KEY}=")]
    if not all_partitions:
        return None

    # 2. 遍历分区目录，找到包含文件的最新日期
    for p_key in sorted(all_partitions, reverse=True):
        date_str = p_key.split('=')[1]
        # 修正：确保分区文件夹下有目标 CSV 文件
        if glob.glob(os.path.join(OUTPUT_DIR, p_key, f"*.{OUTPUT_FORMAT}")):
            write_log(f"已识别出最新已完成采集日期分区: {date_str}。", "INFO")
            return date_str
    
    return None

def get_trade_dates(start_date, end_date):
    """读取交易日历，获取指定范围内的交易日列表。"""
    if not os.path.exists(CALENDAR_PATH):
        fatal_exit(f"交易日历文件不存在，请先运行 01_交易日历.py。路径: {CALENDAR_PATH}")
    
    try:
        # 只读取 A 股相关交易所的交易日
        df_cal = pd.read_csv(CALENDAR_PATH, usecols=['cal_date', 'is_open', 'exchange'])
        df_trade_dates = df_cal[
            (df_cal['exchange'].isin(['SSE', 'SZSE', 'BSE'])) & 
            (pd.to_numeric(df_cal['is_open'], errors='coerce') == 1)
        ]['cal_date'].astype(str).tolist() 
        
        return sorted([d for d in df_trade_dates if d >= start_date and d <= end_date])
    except Exception as e:
        fatal_exit(f"读取或解析交易日历文件失败: {e}。")

def save_data_to_partition_csv(df, partition_value):
    """
    保存数据到交易日分区 CSV 文件，并执行幂等性检查（去重）。
    """
    # ... (与之前一致，省略以保持简洁) ...
    # 1. 检查字段完整性
    missing_cols = list(set(OUTPUT_FIELDS) - set(df.columns))
    if missing_cols:
        write_log(f"T3 断言失败: API 返回数据缺少关键字段 {missing_cols}，请检查接口权限。", "WARNING")
        cols_to_save = [col for col in OUTPUT_FIELDS if col in df.columns]
    else:
        cols_to_save = OUTPUT_FIELDS

    df_working = df[cols_to_save].copy() 

    partition_dir = os.path.join(OUTPUT_DIR, f"{PARTITION_KEY}={partition_value}")
    os.makedirs(partition_dir, exist_ok=True)
    
    output_path = os.path.join(partition_dir, f"{INTERFACE_NAME}.{OUTPUT_FORMAT}")

    try:
        if os.path.exists(output_path):
            df_existing = pd.read_csv(output_path, dtype=str)
            df_existing = df_existing[[col for col in cols_to_save if col in df_existing.columns]].copy()
            
            # 合并新旧数据，保留新数据 (去重逻辑)
            df_working['is_new'] = True
            df_existing['is_new'] = False 
            
            df_combined = pd.concat([df_existing, df_working], ignore_index=True)
            df_unique = df_combined.drop_duplicates(subset=PK_COLUMNS, keep='last').copy()
            
            updated_rows = len(df_unique) - len(df_existing.drop_duplicates(subset=PK_COLUMNS))
            
            if len(df_unique) > 0:
                df_unique.drop(columns=['is_new'], inplace=True) 
                df_unique.to_csv(
                    output_path, 
                    index=False, 
                    mode='w', 
                    header=True,
                    encoding='utf-8'
                )
                write_log(f"分区 {partition_value} 成功覆盖/更新 {updated_rows} 行，当前总行数 {len(df_unique)}。", "DEBUG")
                return updated_rows
            
            return 0
        else:
            # 文件不存在，直接写入
            rows_to_save = len(df_working)
            if rows_to_save > 0:
                df_working.to_csv(
                    output_path, 
                    index=False, 
                    mode='w', 
                    header=True,
                    encoding='utf-8'
                )
                write_log(f"分区 {partition_value} 成功写入 {len(df_working)} 行。", "DEBUG")
                return len(df_working)
            return 0
    except Exception as e:
        fatal_exit(f"保存分区 {partition_value} 失败: {e}")

# -------------------------- T8: 主采集函数 --------------------------

def fetch_and_save_by_date(date_str):
    """
    采集单个交易日的开盘竞价成交数据。
    注意：API 频率控制已外置到主循环。
    """
    
    df_day = None
    
    # T6: 接口调用与重试
    for attempt in range(MAX_RETRY):
        try:
            # Tushare 接口调用
            df_day = PRO_API.stk_auction(
                trade_date=date_str, 
                fields=','.join(OUTPUT_FIELDS)
            )
            break 
        except Exception as e:
            # 频率警告已在主循环中处理
            if attempt < MAX_RETRY - 1:
                write_log(f"日期 {date_str} API 失败，重试 {attempt + 1}/{MAX_RETRY}: {e}", "WARNING")
                # 重试时采用小幅指数退避
                time.sleep(0.5 * (2 ** attempt)) 
            else:
                fatal_exit(f"日期 {date_str} API 最终失败: {e}") 
        
        # 核心频率控制已外置，此处不进行 sleep
        # time.sleep(API_SLEEP_SECONDS) # <--- REMOVED

    if df_day is None or df_day.empty:
        write_log(f"日期 {date_str} API 返回空数据。", "DEBUG")
        return 0

    # T9: 检查重复主键
    if df_day.duplicated(subset=PK_COLUMNS).any():
        write_log(f"WARNING: 日期 {date_str} 存在重复主键，已去重。", "WARNING")
        df_day = df_day.drop_duplicates(subset=PK_COLUMNS, keep='last')
        
    # 3. 保存数据
    rows_saved = save_data_to_partition_csv(df_day, date_str)
    
    return rows_saved

# -------------------------- T1: 主逻辑运行 --------------------------
if __name__ == "__main__":
    _start_date = DEFAULT_START_DATE
    _end_date = datetime.now().strftime('%Y%m%d')

    write_log(f"--- 脚本 {SCRIPT_NAME} 启动 ---")

    # 1. 检查已完成交易日列表（隐式状态）
    latest_completed_date = get_latest_completed_date_from_disk()
    
    # 2. 获取目标交易日列表
    if latest_completed_date:
        # 从已完成日期的下一天开始采集
        start_date_to_fetch = (datetime.strptime(latest_completed_date, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
        _start_date = max(_start_date, start_date_to_fetch)

    dates_to_fetch = get_trade_dates(_start_date, _end_date)
        
    if not dates_to_fetch:
        write_log(f"所有交易日 ({_end_date}) 均已采集完成，无需更新。", "INFO")
        sys.exit(0)
        
    write_log(f"采集范围: {dates_to_fetch[0]} - {dates_to_fetch[-1]}，共 {len(dates_to_fetch)} 个交易日。", "INFO")
    write_log(f"警告：API 频率已设置为 {60/API_SLEEP_SECONDS:.0f} 次/分钟 ({API_SLEEP_SECONDS:.2f} 秒/次)。请确保您的 Tushare 积分权限支持此频率。", "WARNING")

    total_saved_rows = 0
    
    # T1: 逐交易日采集并保存
    pbar = tqdm(dates_to_fetch, desc=f"Fetching {INTERFACE_NAME}")
    for date_str in pbar:
        try:
            pbar.set_postfix({'trade_date': date_str})
            rows_saved = fetch_and_save_by_date(date_str)
            total_saved_rows += rows_saved
            
            # T7: 遵守频率 (已外置到主循环)
            time.sleep(API_SLEEP_SECONDS) 
            
        except Exception as e:
            # 这里的异常通常是 fatal_exit 抛出的
            write_log(f" FATAL ERROR during {date_str} fetching: {e}", "FATAL")
            # 如果不是 fatal_exit 抛出的，则跳过当前日期，继续下一天
            pass 

    write_log(f"全量采集/续采完成。总计保存行数: {total_saved_rows}。", "INFO")
    write_log(f"--- 脚本 {SCRIPT_NAME} 完成 ---")