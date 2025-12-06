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
# 新名称：44_融资融券标的（盘前）.py
SCRIPT_NAME = "44_融资融券标的（盘前）"
INTERFACE_NAME = "margin_secs" # 接口：margin_secs
PARTITION_KEY = 'trade_date' 
# 新目录：D:\2025\raw\股票数据\两融及转融通\融资融券标的（盘前）\
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '两融及转融通', '融资融券标的（盘前）') 
OUTPUT_FORMAT = 'csv' 

# 全局工程配置
BASE_DIR = r"D:\2025" 
TODAY_DATE_STR = datetime.now().strftime('%Y%m%d')
MAX_RETRY = 3 
# margin_secs 接口限制 5000次/分钟。设置为 0.35 秒的安全间隔。
API_SLEEP_SECONDS = 0.35 
PAGE_SIZE = 8000 # 接口没有分页参数，但保留此变量作为标准配置
DEFAULT_START_DATE = "19900101" 

# 所需字段
FIELDS = "trade_date,ts_code,name,exchange"


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


# -------------------------- T2: 通用函数 (适配控制台输出) --------------------------

def console_print(message: str, level: str = "INFO"):
    """替换 log 文件输出，直接打印到控制台/终端。"""
    print(f"[{level:<5}] {SCRIPT_NAME}: {message}")

def fatal_exit(message: str):
    """打印致命错误信息并退出。"""
    console_print(message, "FATAL")
    sys.exit(1)

def get_latest_completed_date_from_disk() -> str:
    """
    检查输出目录，返回已采集的最新分区日期。
    """
    try:
        search_path = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR, '*')
        all_partition_dirs = glob.glob(search_path)
        
        dates = [os.path.basename(d) for d in all_partition_dirs if os.path.isdir(d)]
        
        if not dates:
            return ""

        latest_date = max(dates, default="")
        
        if latest_date:
            file_path = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR, latest_date, f"{SCRIPT_NAME}_{latest_date}.{OUTPUT_FORMAT}")
            if os.path.exists(file_path):
                console_print(f"已识别出最新已完成采集日期分区: {latest_date}。", "INFO")
                return latest_date
            else:
                dates.remove(latest_date)
                if dates:
                    latest_date = max(dates)
                    console_print(f"最新分区 {latest_date} 文件缺失，回溯到前一个有效日期: {latest_date}。", "WARNING")
                    return latest_date
                
        return "" 
    
    except Exception as e:
        console_print(f"检查本地数据时出错: {e}", "ERROR")
        return ""

def get_trade_dates(start_date: str, end_date: str) -> List[str]:
    """
    从 Tushare 获取指定范围内的所有交易日列表。
    """
    try:
        console_print(f"正在从 Tushare 获取交易日历...")
        df_cal = PRO_API.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
        trade_dates = df_cal[df_cal['is_open'] == 1]['cal_date'].tolist()
        
        if not trade_dates:
            console_print(f"在 {start_date} 到 {end_date} 范围内未找到交易日。", "WARNING")
            return []
            
        return sorted(trade_dates)
        
    except Exception as e:
        fatal_exit(f"获取交易日历失败: {e}")

def save_data_to_partition_csv(df: pd.DataFrame, date_str: str) -> int:
    """
    将 DataFrame 存入按日期分区的 CSV 文件。
    """
    if df.empty:
        console_print(f"日期 {date_str} API 返回空数据。", "INFO")
        return 0
        
    try:
        partition_dir = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR, date_str)
        os.makedirs(partition_dir, exist_ok=True)
        
        file_path = os.path.join(partition_dir, f"{SCRIPT_NAME}_{date_str}.{OUTPUT_FORMAT}")
        df.to_csv(file_path, index=False, encoding='utf-8')
        
        rows_saved = len(df)
        console_print(f"日期 {date_str} 成功保存 {rows_saved} 行数据。", "INFO")
        return rows_saved
        
    except Exception as e:
        console_print(f"保存数据到磁盘失败 ({date_str}): {e}", "ERROR")
        return 0


# -------------------------- T1: 核心采集逻辑 --------------------------

def fetch_and_save_by_date(date_str: str) -> int:
    """
    按交易日调用 Tushare margin_secs 接口采集数据。
    """
    console_print(f"开始采集交易日 {date_str}...")
    
    for attempt in range(MAX_RETRY):
        try:
            # 核心 API 调用
            df = PRO_API.margin_secs(
                trade_date=date_str,
                fields=FIELDS
            )
            
            if df.empty:
                console_print(f"交易日 {date_str} API 返回空数据。", "INFO")
                return 0
                
            # 保存数据
            return save_data_to_partition_csv(df, date_str)
            
        except Exception as e:
            if attempt < MAX_RETRY - 1:
                console_print(f"API 失败，重试 {attempt + 1}/{MAX_RETRY}: {e}", "WARNING")
                time.sleep(API_SLEEP_SECONDS * (2 ** attempt)) 
            else:
                console_print(f"交易日 {date_str} API 最终失败: {e}", "ERROR")
                return 0 # 失败后跳过当前日期


# -------------------------- T3: 主逻辑运行 --------------------------
if __name__ == "__main__":
    _start_date = DEFAULT_START_DATE
    _end_date = TODAY_DATE_STR

    console_print(f"--- 脚本 {SCRIPT_NAME} 启动 (支持断点续采/CSV，控制台输出) ---")

    # 1. 检查已完成交易日列表（隐式状态）
    latest_completed_date = get_latest_completed_date_from_disk()
    
    # 2. 获取目标交易日列表
    if latest_completed_date:
        start_date_to_fetch = (datetime.strptime(latest_completed_date, '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d')
        _start_date = max(_start_date, start_date_to_fetch)

    dates_to_fetch = get_trade_dates(_start_date, _end_date)
        
    if not dates_to_fetch:
        console_print(f"所有交易日 ({_end_date}) 均已采集完成，无需更新。", "INFO")
        sys.exit(0)
        
    console_print(f"采集范围: {dates_to_fetch[0]} - {dates_to_fetch[-1]}，共 {len(dates_to_fetch)} 个交易日。", "INFO")

    total_saved_rows = 0
    
    # T1: 逐交易日采集并保存
    pbar = tqdm(dates_to_fetch, desc=f"Fetching {SCRIPT_NAME}", unit="day")
    for date_str in pbar:
        pbar.set_postfix({'date': date_str})
        
        # 调用核心采集函数
        rows_saved = fetch_and_save_by_date(date_str)
        total_saved_rows += rows_saved
        
        # T7: 遵守频率，外置于主循环
        time.sleep(API_SLEEP_SECONDS) 

    console_print(f"--- 脚本 {SCRIPT_NAME} 运行结束。总共保存 {total_saved_rows} 行数据。 ---")