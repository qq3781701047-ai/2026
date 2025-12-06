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
SCRIPT_NAME = "32_个股资金流向"
INTERFACE_NAME = "moneyflow"
PARTITION_KEY = 'trade_date' 
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '资金流向数据', '个股资金流向') 
OUTPUT_FORMAT = 'csv' 

# 全局工程配置
BASE_DIR = r"D:\2025" 
TODAY_DATE_STR = datetime.now().strftime('%Y%m%d')
MAX_RETRY = 3 
# Tushare moneyflow 接口限制 5000次/分钟。设置为 0.35 秒的安全间隔。
API_SLEEP_SECONDS = 0.35 
# moneyflow 接口默认分页大小 10000，设置为 8000 避免数据过多。
PAGE_SIZE = 8000 
# 从证券交易所开市之日起采集，我们使用一个保守的较早日期。
DEFAULT_START_DATE = "19900101" 

# Tushare moneyflow 接口字段清单 (已包含用户要求的所有字段)
FIELDS = "ts_code,trade_date,buy_sm_vol,buy_sm_amount,sell_sm_vol,sell_sm_amount,buy_md_vol,buy_md_amount,sell_md_vol,sell_md_amount,buy_lg_vol,buy_lg_amount,sell_lg_vol,sell_lg_amount,buy_elg_vol,buy_elg_amount,sell_elg_vol,sell_elg_amount,net_mf_vol,net_mf_amount,trade_count"


# -------------------------- Tushare 配置与初始化 --------------------------
# 请替换为您的 Tushare Token
TS_TOKEN = "374f3850d22ecc9f328be05c8f655f274ed5ff0c2c8a45f39fd76092" # <--- 请在此处替换为你的 Tushare Token
try:
    ts.set_token(TS_TOKEN)
    # T7: 增加 API 请求超时时间为 60 秒
    PRO_API = ts.pro_api(timeout=60) 
except Exception as e:
    print(f"FATAL ERROR: Tushare Token 配置失败或未配置。详细错误: {e}")
    sys.exit(1)
# -------------------------------------------------------------------------


# -------------------------- T2: 通用函数 (适配控制台输出) --------------------------

def console_print(message: str, level: str = "INFO"):
    """替换 log 文件输出，直接打印到控制台/终端。"""
    # 打印格式 [级别] SCRIPT_NAME: 消息
    print(f"[{level:<5}] {SCRIPT_NAME}: {message}")

def fatal_exit(message: str):
    """打印致命错误信息并退出。"""
    console_print(message, "FATAL")
    sys.exit(1)

def get_latest_completed_date_from_disk() -> str:
    """
    检查输出目录，返回已采集的最新分区日期。
    目录结构: BASE_DIR/raw/OUTPUT_BASE_SUBDIR/date_str/SCRIPT_NAME_date_str.csv
    """
    try:
        # D:\2025\raw\股票数据\资金流向数据\个股资金流向\*
        search_path = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR, '*')
        
        # 查找所有分区文件夹
        all_partition_dirs = glob.glob(search_path)
        
        # 提取文件夹名称 (即日期字符串)
        dates = [os.path.basename(d) for d in all_partition_dirs if os.path.isdir(d)]
        
        if not dates:
            return "" # 首次采集，没有已完成日期

        # 找到最新的日期
        latest_date = max(dates, default="")
        
        if latest_date:
            # 检查最新的日期分区下是否有文件，如果没有文件，则忽略该日期（视为未完成）
            file_path = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR, latest_date, f"{SCRIPT_NAME}_{latest_date}.{OUTPUT_FORMAT}")
            if os.path.exists(file_path):
                console_print(f"已识别出最新已完成采集日期分区: {latest_date}。", "INFO")
                return latest_date
            else:
                # 文件夹存在但文件缺失，我们应该回溯到上一个有效的日期
                dates.remove(latest_date)
                if dates:
                    latest_date = max(dates)
                    console_print(f"最新分区 {latest_date} 文件缺失，回溯到前一个有效日期: {latest_date}。", "WARNING")
                    return latest_date
                
        return "" # 依然没有有效日期
    
    except Exception as e:
        console_print(f"检查本地数据时出错: {e}", "ERROR")
        return ""

def get_trade_dates(start_date: str, end_date: str) -> List[str]:
    """
    从 Tushare 获取指定范围内的所有交易日列表。
    """
    try:
        console_print(f"正在从 Tushare 获取交易日历...")
        # 尝试从 Tushare 获取交易日历
        df_cal = PRO_API.trade_cal(exchange='SSE', start_date=start_date, end_date=end_date)
        
        # 过滤出交易日 (is_open == 1) 并提取日期
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
        # 创建分区目录: D:\2025\raw\股票数据\资金流向数据\个股资金流向\20250101
        partition_dir = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR, date_str)
        os.makedirs(partition_dir, exist_ok=True)
        
        # 文件路径: D:\2025\raw\股票数据\资金流向数据\个股资金流向\20250101\32_个股资金流向_20250101.csv
        file_path = os.path.join(partition_dir, f"{SCRIPT_NAME}_{date_str}.{OUTPUT_FORMAT}")
        
        # 保存为 CSV
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
    按交易日调用 Tushare moneyflow 接口并分页采集全量数据。
    """
    console_print(f"开始采集交易日 {date_str}...")
    
    all_data = []
    offset = 0
    current_page_rows = 0 
    
    while True:
        for attempt in range(MAX_RETRY):
            try:
                # 核心 API 调用
                df = PRO_API.moneyflow(
                    trade_date=date_str,
                    limit=PAGE_SIZE,
                    offset=offset,
                    fields=FIELDS
                )
                
                if df.empty:
                    current_page_rows = 0
                    break
                    
                all_data.append(df)
                current_page_rows = len(df)
                
                # 如果返回的数据量小于分页大小，说明已经是最后一页
                if current_page_rows < PAGE_SIZE:
                    break
                
                # 准备下一页
                offset += PAGE_SIZE
                
                # T7: 遵守频率，只有在需要拉取下一页时才休眠
                time.sleep(API_SLEEP_SECONDS) 
                
            except Exception as e:
                if attempt < MAX_RETRY - 1:
                    console_print(f"API 失败，重试 {attempt + 1}/{MAX_RETRY}: {e}", "WARNING")
                    time.sleep(API_SLEEP_SECONDS * (2 ** attempt)) 
                else:
                    console_print(f"交易日 {date_str} API 最终失败: {e}", "ERROR")
                    return 0 # 失败后跳过当前日期

        # 如果当前页没有数据 (API 返回空或最后一页)，跳出外层循环
        if current_page_rows == 0 or current_page_rows < PAGE_SIZE:
            break
            
        # 如果达到了最大重试次数，也跳出（错误已在内层处理）
        if attempt == MAX_RETRY - 1 and current_page_rows > 0:
             # 如果重试成功且返回了数据，应该继续下一页，这里逻辑上已由内层循环的 break 保证。
             # 只有当 current_page_rows < PAGE_SIZE 时才真正退出。
             pass

    if not all_data:
        console_print(f"交易日 {date_str} API 返回空数据。", "INFO")
        return 0

    df_result = pd.concat(all_data, ignore_index=True)
    console_print(f"交易日 {date_str} 采集完成，总共采集 {len(df_result)} 行。", "INFO")
    
    # 保存数据
    return save_data_to_partition_csv(df_result, date_str)


# -------------------------- T3: 主逻辑运行 --------------------------
if __name__ == "__main__":
    _start_date = DEFAULT_START_DATE
    _end_date = TODAY_DATE_STR

    console_print(f"--- 脚本 {SCRIPT_NAME} 启动 (支持断点续采/CSV，控制台输出) ---")

    # 1. 检查已完成交易日列表（隐式状态）
    latest_completed_date = get_latest_completed_date_from_disk()
    
    # 2. 获取目标交易日列表
    if latest_completed_date:
        # 从已完成日期的下一天开始采集
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
        
        # T7: 遵守频率，外置于主循环，用于控制日期间的采集间隔。
        # (分页内的休眠已在 fetch_and_save_by_date 函数内部控制)
        time.sleep(API_SLEEP_SECONDS) 

    console_print(f"--- 脚本 {SCRIPT_NAME} 运行结束。总共保存 {total_saved_rows} 行数据。 ---")