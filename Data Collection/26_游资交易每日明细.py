import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import tushare as ts
from tqdm import tqdm 
import time
import glob
import logging
from typing import List

# -------------------------- T0: 元信息 (Meta Info) --------------------------
SCRIPT_NAME = "26_游资交易每日明细"
INTERFACE_NAME = "hm_detail"
PARTITION_KEY = 'trade_date' # T4: 分区键，按交易日期分区
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '打板专题数据', '游资交易每日明细') 
OUTPUT_FORMAT = 'csv' 

# 全局工程配置
BASE_DIR = r"D:\2025" 
TODAY_DATE_STR = datetime.now().strftime('%Y%m%d')
MAX_RETRY = 3 
# 【！！！核心修正！！！】Tushare 提示为 2次/小时，安全休眠时间设置为 1801.0 秒 (30 分钟 + 1 秒)
API_SLEEP_SECONDS = 1801.0
PAGE_SIZE = 2000 # 接口单次最大 2000 条
DEFAULT_START_DATE = "20220801" # 接口数据开始于 2022 年 8 月，故设置此日期

# -------------------------- Tushare 配置与初始化 --------------------------
# 请替换为您的 Tushare Token
TS_TOKEN = "374f3850d22ecc9f328be05c8f655f274ed5ff0c2c8a45f39fd76092" # <--- 请在此处替换为你的 Tushare Token
try:
    ts.set_token(TS_TOKEN)
    PRO_API = ts.pro_api()
except Exception:
    print("FATAL ERROR: Tushare Token 配置失败或未配置。")
    sys.exit(1)

# -------------------------- T3: 字段契约 (Contract) --------------------------
OUTPUT_FIELDS = [
    'trade_date', 'ts_code', 'ts_name', 'buy_amount', 'sell_amount', 
    'net_amount', 'tag', 'hm_name', 'hm_orgs'
]
PK_COLUMNS = ['trade_date', 'ts_code', 'hm_name'] 

# -------------------------- T6/T7/T11/T10: 核心工程函数 --------------------------
def get_output_dir() -> str:
    """T4: 获取脚本的输出路径 D:\\2025\\raw\\...\\脚本名\\"""
    return os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)

def write_log(message: str, level: str = "INFO", exit_script: bool = False):
    """T7: 写入运行日志文件。（已修正 NameError 作用域）"""
    log_dir = os.path.join(BASE_DIR, 'logs', TODAY_DATE_STR)
    log_file_path = os.path.join(log_dir, f"{SCRIPT_NAME}.log") 
    os.makedirs(log_dir, exist_ok=True)
    
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    logger = logging.getLogger(SCRIPT_NAME) 
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fh = logging.FileHandler(log_file_path, encoding='utf-8')
        fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(fh)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
        logger.addHandler(sh)

    logger.log(log_level, f"{SCRIPT_NAME}: {message}") 
    
    if exit_script:
        sys.exit(1)

def fatal_exit(message: str):
    """T11: 致命错误，退出脚本并记录日志。"""
    write_log(message, "FATAL", exit_script=True)

def get_latest_completed_date_from_disk() -> str:
    """T10: 从磁盘读取已完成的分区日期，实现断点续采。（已修正参数）"""
    output_dir = get_output_dir()
    pattern = os.path.join(output_dir, f"{PARTITION_KEY}=*", f"*.{OUTPUT_FORMAT}")
    
    completed_dates = []
    for path in glob.glob(pattern):
        try:
            date_str = path.split(f'{PARTITION_KEY}=')[-1].split(os.sep)[0]
            if len(date_str) == 8 and date_str.isdigit():
                completed_dates.append(date_str)
        except:
            continue
            
    if not completed_dates:
        return None
        
    return max(completed_dates)

def get_trade_dates(start_date: str, end_date: str) -> List[str]:
    """获取指定范围内的所有交易日。"""
    try:
        df_dates = PRO_API.trade_cal(exchange='SSE', is_open='1', 
                                     start_date=start_date, end_date=end_date)
        
        today_str = datetime.now().strftime('%Y%m%d')
        df_dates = df_dates[df_dates['cal_date'] <= today_str]
        
        if df_dates.empty:
            write_log(f"Tushare trade_cal 在 [{start_date}, {end_date}] 范围内返回空交易日。", "WARNING")
            return []
            
        return sorted(df_dates['cal_date'].tolist())
    except Exception as e:
        fatal_exit(f"无法获取交易日历：{e}")
        return []

def save_data_to_partition_csv(script_name: str, df: pd.DataFrame, partition_value: str) -> int:
    """T4/T5/T9: 检查数据完整性，并保存到 CSV 分区文件 (T10: 幂等覆盖)。"""
    if df.empty:
        write_log(f"日期 {partition_value} 无数据，跳过保存。", "DEBUG")
        return 0

    if df.duplicated(subset=PK_COLUMNS).any():
        fatal_exit(f"T9 断言失败: 日期 {partition_value} 存在重复主键，违反唯一性原则。")

    missing_cols = list(set(OUTPUT_FIELDS) - set(df.columns))
    if missing_cols:
        fatal_exit(f"T3 断言失败: API 返回数据缺少关键字段 {missing_cols}，请检查接口权限。")

    df = df[OUTPUT_FIELDS] 

    output_dir = get_output_dir()
    partition_dir = os.path.join(output_dir, f"{PARTITION_KEY}={partition_value}")
    os.makedirs(partition_dir, exist_ok=True)
    output_path = os.path.join(partition_dir, f"{script_name.split('_', 1)[1]}.{OUTPUT_FORMAT}")
    
    try:
        df.to_csv(output_path, index=False, encoding='utf-8')
        write_log(f"日期 {partition_value} 成功保存 {len(df)} 行。", "INFO")
        return len(df)
    except Exception as e:
        fatal_exit(f"保存分区 {partition_value} 失败: {e}")
        return 0

def fetch_and_save_by_date(date_str: str) -> int:
    """T1: 核心采集逻辑：按交易日采集，并处理分页。"""
    offset = 0
    all_data = []

    write_log(f"开始采集交易日 {date_str}...", "INFO")

    # T1: 分页循环采集
    while True:
        df_day = None
        current_page_rows = 0 
        
        for attempt in range(MAX_RETRY):
            try:
                # Tushare 接口调用：hm_detail
                df_day = PRO_API.hm_detail(
                    trade_date=date_str,
                    fields=','.join(OUTPUT_FIELDS),
                    limit=PAGE_SIZE,
                    offset=offset
                )
                
                if df_day is not None and not df_day.empty:
                    df_day = df_day.rename(columns={'trade_date': PARTITION_KEY})
                    all_data.append(df_day)
                    current_page_rows = len(df_day)
                    break 
                else:
                    current_page_rows = 0
                    break 
            
            except Exception as e:
                # 频率限制处理：如果失败，强制等待 1801.0 秒后重试
                if attempt < MAX_RETRY - 1:
                    write_log(f"API 失败，重试 {attempt + 1}/{MAX_RETRY}: {e}", "WARNING")
                    # 在重试前强制休眠 API_SLEEP_SECONDS
                    time.sleep(API_SLEEP_SECONDS) 
                else:
                    fatal_exit(f"日期 {date_str} API 最终失败: {e}")
            
        
        # 检查是否还有下一页
        if current_page_rows < PAGE_SIZE:
            break 
        
        offset += PAGE_SIZE

    if not all_data:
        write_log(f"日期 {date_str} API 返回空数据。", "INFO")
        return 0

    df_result = pd.concat(all_data, ignore_index=True)
    write_log(f"日期 {date_str} 分页采集完成，总共采集 {len(df_result)} 行。", "INFO")
    
    original_rows = len(df_result)
    df_result.drop_duplicates(subset=PK_COLUMNS, keep='first', inplace=True) 
    if len(df_result) < original_rows:
        write_log(f"日期 {date_str} 发现 {original_rows - len(df_result)} 条重复主键数据，已去重。", "WARNING")
    
    return save_data_to_partition_csv(SCRIPT_NAME, df_result, date_str)

# -------------------------- T1: 主逻辑运行 --------------------------
if __name__ == "__main__":
    _start_date = DEFAULT_START_DATE
    _end_date = datetime.now().strftime('%Y%m%d')

    write_log(f"--- 脚本 {SCRIPT_NAME} 启动 ---")

    # 1. 检查已完成交易日列表（隐式状态）
    latest_completed_date = get_latest_completed_date_from_disk()
    
    # 2. 获取目标交易日列表
    if latest_completed_date:
        start_date_to_fetch = (datetime.strptime(latest_completed_date, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
        _start_date = max(_start_date, start_date_to_fetch)

    dates_to_fetch = get_trade_dates(_start_date, _end_date)
        
    if not dates_to_fetch:
        write_log(f"所有交易日 ({_end_date}) 均已采集完成，无需更新。", "INFO")
        sys.exit(0)
        
    write_log(f"采集范围: {dates_to_fetch[0]} - {dates_to_fetch[-1]}，共 {len(dates_to_fetch)} 个交易日。", "INFO")

    total_saved_rows = 0
    
    # T1: 逐交易日采集并保存
    for date_str in tqdm(dates_to_fetch, desc=f"Fetching {INTERFACE_NAME}"):
        try:
            rows_saved = fetch_and_save_by_date(date_str)
            total_saved_rows += rows_saved
        except Exception as e:
            # fetch_and_save_by_date 内部已经处理了致命错误退出
            write_log(f"FATAL ERROR during {date_str} fetching: {e}", "FATAL")
        
        # 【！！！主循环休眠！！！】
        # 每次成功获取一个交易日的数据后，执行休眠 1801.0 秒，确保下一个交易日调用不受频率限制
        time.sleep(API_SLEEP_SECONDS)


    write_log(f"--- 脚本 {SCRIPT_NAME} 运行结束。总共保存 {total_saved_rows} 行数据。---", "INFO")