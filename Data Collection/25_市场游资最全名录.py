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
SCRIPT_NAME = "25_市场游资最全名录"
INTERFACE_NAME = "hm_list"
PARTITION_KEY = 'stat_date' # T4: 分区键，使用统计日期/采集日期作为版本
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '打板专题数据', '市场游资最全名录') 
OUTPUT_FORMAT = 'csv' 

# 全局工程配置
BASE_DIR = r"D:\2025" 
TODAY_DATE_STR = datetime.now().strftime('%Y%m%d')
MAX_RETRY = 3 
# T7: 遵守频率：hm_list 接口限制 2次/小时，设置为 1800.1 秒/次
API_SLEEP_SECONDS = 1800.1
PAGE_SIZE = 1000 # 接口单次最大 1000 条
# 修正为证券交易所成立日期，确保全量回溯
DEFAULT_START_DATE = "19901219" 

# 交易日历文件的位置，用于确定时间轴 (假设文件已存在)
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
    'name', 'desc', 'orgs'
]
PK_COLUMNS = ['name'] # 主键：游资名称

# -------------------------- T6/T7/T11/T10: 核心工程函数 --------------------------

def setup_directories():
    """初始化输出目录。"""
    output_dir = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

OUTPUT_DIR = setup_directories()

def write_log(message: str, level: str = "INFO"):
    """T7: 仅打印到控制台，不再写入日志文件。"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # 打印到控制台
    print(f"[{timestamp}] [{level}] {SCRIPT_NAME.split('_', 1)[1]}: {message}")

def fatal_exit(message: str):
    """T11: 记录致命错误并退出 (零缺失硬停止)。"""
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
    # 注意：这里我们使用 partition_key 的值进行判断，也就是日期
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
        df_cal = pd.read_csv(CALENDAR_PATH, usecols=['cal_date', 'is_open', 'exchange'])
        df_trade_dates = df_cal[
            (df_cal['exchange'].isin(['SSE', 'SZSE', 'BSE'])) & 
            (pd.to_numeric(df_cal['is_open'], errors='coerce') == 1)
        ]['cal_date'].astype(str).tolist() 
        
        return sorted([d for d in df_trade_dates if d >= start_date and d <= end_date])
    except Exception as e:
        fatal_exit(f"读取或解析交易日历文件失败: {e}。")

def save_data_to_partition_csv(df: pd.DataFrame, partition_value: str) -> int:
    """
    保存数据到交易日分区 CSV 文件，实现幂等性（直接覆盖）。
    """
    if df.empty:
        write_log(f"游资名录返回空数据，跳过保存。", "WARNING")
        return 0

    # T3: 字段契约检查
    df_working = df[OUTPUT_FIELDS].copy()

    # T9: 六断言 - 检查重复主键（Tushare API返回的数据自身不应有重复的name）
    if df_working.duplicated(subset=PK_COLUMNS).any():
        fatal_exit(f"T9 断言失败: API 返回数据中存在重复主键 {PK_COLUMNS}，违反唯一性原则。")

    partition_dir = os.path.join(OUTPUT_DIR, f"{PARTITION_KEY}={partition_value}")
    os.makedirs(partition_dir, exist_ok=True)
    
    # 游资名录的最终文件名
    output_path = os.path.join(partition_dir, f"{INTERFACE_NAME}.{OUTPUT_FORMAT}")
    
    # 直接写入覆盖既有文件 (T10: 幂等)
    try:
        df_working.to_csv(
            output_path, 
            index=False, 
            mode='w', 
            header=True,
            encoding='utf-8'
        )
        write_log(f"分区 {partition_value} 成功保存 {len(df_working)} 行。", "DEBUG")
        return len(df_working)
    except Exception as e:
        fatal_exit(f"保存分区 {partition_value} 失败: {e}")
        return 0

# -------------------------- T8: 主采集函数 --------------------------

def fetch_and_save_by_date(date_str: str) -> int:
    """
    采集当日的游资名录数据并保存为该日期的快照。
    注意：API 频率控制已外置到主循环。
    """
    
    df_list = None
    
    # T6: 接口调用与重试
    for attempt in range(MAX_RETRY):
        try:
            # Tushare 接口调用：hm_list (此接口不接受 trade_date 参数，返回当前最新快照)
            df_list = PRO_API.hm_list(
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
    
    if df_list is None or df_list.empty:
        write_log(f"日期 {date_str} API 返回空数据。", "DEBUG")
        return 0
        
    # 保存数据，使用 date_str 作为分区键
    rows_saved = save_data_to_partition_csv(df_list, date_str)
    
    return rows_saved

# -------------------------- T1: 主逻辑运行 --------------------------
if __name__ == "__main__":
    _start_date = DEFAULT_START_DATE
    _end_date = datetime.now().strftime('%Y%m%d')

    write_log(f"--- 脚本 {SCRIPT_NAME} 启动 (按日快照采集) ---")

    # 1. 检查已完成交易日列表（隐式状态）
    latest_completed_date = get_latest_completed_date_from_disk()
    
    # 2. 获取目标交易日列表
    if latest_completed_date:
        # 断点续采：从已完成日期的下一天开始采集
        start_date_to_fetch = (datetime.strptime(latest_completed_date, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
        _start_date = max(_start_date, start_date_to_fetch)

    dates_to_fetch = get_trade_dates(_start_date, _end_date)
        
    if not dates_to_fetch:
        write_log(f"所有交易日 ({_end_date}) 均已采集完成，无需更新。", "INFO")
        sys.exit(0)
        
    write_log(f"采集范围: {dates_to_fetch[0]} - {dates_to_fetch[-1]}，共 {len(dates_to_fetch)} 个交易日。", "INFO")
    write_log(f"警告：API 频率已设置为 {60/API_SLEEP_SECONDS:.0f} 次/分钟 ({API_SLEEP_SECONDS:.3f} 秒/次)。", "WARNING")

    total_saved_rows = 0
    
    # T1: 逐交易日采集并保存
    pbar = tqdm(dates_to_fetch, desc=f"Fetching {INTERFACE_NAME}")
    for date_str in pbar:
        try:
            pbar.set_postfix({'stat_date': date_str})
            # T1: 核心采集调用
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