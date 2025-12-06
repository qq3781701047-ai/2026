import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import numpy as np
import tushare as ts
from tqdm import tqdm 
import glob
import time

# -------------------------- Tushare 配置 (保持一致) --------------------------
# !!! 注意: 需与之前脚本使用相同的真实 Tushare Token !!!
TS_TOKEN = "374f3850d22ecc9f328be05c8f655f274ed5ff0c2c8a45f39fd76092" 
try:
    ts.set_token(TS_TOKEN)
    PRO_API = ts.pro_api()
except Exception:
    print("FATAL ERROR: Tushare Token 配置失败或未配置。请检查 TS_TOKEN 变量。")
    sys.exit(1)
# ---------------------------------------------------------------------------------------

# --- 新增/修改的工程常量 ---
MAX_RETRY = 3 # T6: 最大重试次数
# T7: stk_limit 接口限制 200 次/分钟，安全间隔 60/200 = 0.3 秒，我们使用 0.35 秒
API_SLEEP_SECONDS = 0.35

# --- 基础配置与通用函数 ---
SCRIPT_NAME = "05_每日涨跌停价格"
BASE_DIR = r"D:\2025"
TODAY_DATE = datetime.now()
TODAY_DATE_STR = TODAY_DATE.strftime('%Y%m%d')
LOG_DIR = os.path.join(BASE_DIR, 'logs', TODAY_DATE_STR)
os.makedirs(LOG_DIR, exist_ok=True) 

# 目标目录结构：按 trade_date 分区
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '行情数据', '每日涨跌停价格')
OUTPUT_DIR = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 交易日历文件的位置，用于确定时间轴
CALENDAR_PATH = os.path.join(BASE_DIR, 'raw', '股票数据', '基础数据', '交易日历', 'trade_calendar_full.csv')

# 最终需要的字段契约 (stk_limit 接口返回的字段)
FIELD_CONTRACT = [
    'ts_code', 'trade_date', 'pre_close', 'up_limit', 
    'down_limit'
    # 注: dro_limit (涨跌幅限制比例) 只有高权限才返回
]
MIN_START_DATE = '19901219' # A股最早上市日期

def write_log(message, level="INFO"):
    """写入运行日志文件。"""
    log_file = os.path.join(LOG_DIR, f"{SCRIPT_NAME}_run.log")
    formatted_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {message}"
    print(formatted_message)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(formatted_message + '\n')

def fetch_trade_dates():
    """读取 01_交易日历 的产物，获取全市场的交易日列表。"""
    write_log(f"检查交易日历文件是否存在: {CALENDAR_PATH}", "INFO")
    if not os.path.exists(CALENDAR_PATH):
        write_log(f"FATAL ERROR: 交易日历文件不存在，请先运行 01_交易日历.py。", "FATAL")
        sys.exit(1)
    
    try:
        # 只读取必要的列
        df_cal = pd.read_csv(CALENDAR_PATH, usecols=['cal_date', 'is_open', 'exchange'])
        
        # 筛选上海交易所 (SSE) 的有效交易日，并确保是开放日
        # 使用 SSE 日历作为全市场的基准交易日
        df_trade_dates = df_cal[
            (df_cal['exchange'] == 'SSE') & 
            (pd.to_numeric(df_cal['is_open'], errors='coerce') == 1)
        ]['cal_date'].astype(str).tolist() 
        
        # 仅保留从 A 股开市日开始的交易日
        return sorted([d for d in df_trade_dates if d >= MIN_START_DATE])
        
    except Exception as e:
        write_log(f"读取或解析交易日历文件失败: {e}。", "FATAL")
        sys.exit(1)

def get_last_collected_date(output_dir):
    """断点续采：获取已落地数据的最新分区日期 (按 trade_date=...)。"""
    # 查找所有 trade_date=... 的目录
    partition_dirs = glob.glob(os.path.join(output_dir, "trade_date=*"))
    
    if not partition_dirs:
        # 如果文件不存在，返回最早日期前一天
        return '19901218' 
    
    # 从目录名中提取日期并找到最大值
    dates = [os.path.basename(d).split('=')[-1] for d in partition_dirs]
    latest_date = max(dates)
    
    if latest_date < MIN_START_DATE or latest_date > TODAY_DATE_STR:
        return '19901218' 

    return latest_date

def save_to_csv_partitioned(df_day, date_str, subdir):
    """数据落地函数，CSV 格式，按 trade_date 分区。"""
    output_base_dir = os.path.join(BASE_DIR, 'raw', subdir)
    
    # 分区目录结构：D:\2025\raw\股票数据\行情数据\每日涨跌停价格\trade_date=YYYYMMDD\data.csv
    partition_dir = os.path.join(output_base_dir, f"trade_date={date_str}")
    os.makedirs(partition_dir, exist_ok=True)
    
    output_path = os.path.join(partition_dir, 'data.csv')
    
    try:
        if df_day.empty:
            return False
            
        # 严格遵守字段契约和顺序
        # 接口可能返回 dro_limit，如果返回则保留，否则忽略
        cols_to_save = [col for col in FIELD_CONTRACT if col in df_day.columns]
        
        df_day[cols_to_save].to_csv(output_path, index=False, encoding='utf-8')
        write_log(f"日期 {date_str} 成功保存 {len(df_day)} 行。", "DEBUG")
        return True
    except Exception as e:
        write_log(f"数据落地失败 (CSV) on {date_str}: {e}", "ERROR")
        return False

def run_05_limit_price():
    write_log(f"--- {SCRIPT_NAME}.py 开始执行 ---", "INFO")
    
    # 步骤 1: 获取交易日历（时间轴）
    all_trade_dates = fetch_trade_dates()
    
    # 步骤 2: 确定断点续采的起始日期
    last_collected_date = get_last_collected_date(OUTPUT_DIR)
    
    start_date_dt = datetime.strptime(last_collected_date, '%Y%m%d') + timedelta(days=1)
    start_date_str = start_date_dt.strftime('%Y%m%d')
    
    # 过滤交易日历，只保留需要采集的日期
    trade_dates_to_pull = [d for d in all_trade_dates if d >= start_date_str and d <= TODAY_DATE_STR]

    write_log(f"增量采集：从 {start_date_str} 开始获取每日涨跌停价格。", "INFO")
    write_log(f"待采集交易日数量: {len(trade_dates_to_pull)}", "INFO") 
    if not trade_dates_to_pull:
        write_log("没有需要采集的新交易日。", "INFO")
        return
        
    # --- 3. 循环交易日，按天调用 API 获取全市场快照并分区落地 ---
    
    write_log(f"开始生成并分区落地 {len(trade_dates_to_pull)} 天的快照 (按天调用 stk_limit 接口)...", "INFO")

    # 使用进度条封装交易日循环
    for date_str in tqdm(trade_dates_to_pull, desc=f"分区落地 [{SCRIPT_NAME}]"):
        
        df_limit = None
        # T6: 引入重试机制
        for attempt in range(MAX_RETRY):
            try:
                # 核心逻辑：按日期调用 API 获取当日全市场数据
                df_limit = PRO_API.stk_limit(trade_date=date_str)
                break # 成功则跳出重试循环
            except Exception as e:
                # 检查是否是 API 频率限制错误
                if "daily limit" in str(e).lower() or "200次" in str(e):
                    # 频率限制是致命的，记录后立即停止
                    write_log(f"API 调用频率限制警告！已停止采集。下次运行将从 {date_str} 处断点续传。", "FATAL")
                    sys.exit(1)

                if attempt < MAX_RETRY - 1:
                    write_log(f"采集 {date_str} 失败，重试 {attempt + 1}/{MAX_RETRY}: {e}", "WARNING")
                    # T7: 指数退避 (Sleep 机制)
                    time.sleep(API_SLEEP_SECONDS * (2 ** attempt)) 
                else:
                    # T11: 零缺失硬停止 - 最终失败
                    write_log(f"致命错误：采集 {date_str} 最终失败: {e}", "FATAL")
                    # 最终失败也退出，确保数据完整性
                    sys.exit(1)
        
        # 如果 API 调用最终失败或返回 None，则跳过本次循环 (由上面的 sys.exit(1) 处理)
        if df_limit is None or df_limit.empty:
            write_log(f"日期 {date_str} 无数据。", "DEBUG")
        else:
            # 步骤 4: 落地数据
            save_to_csv_partitioned(df_limit, date_str, OUTPUT_BASE_SUBDIR)
                
        # T7: 遵守频率 - 无论成功与否，在继续下一次循环前进行休眠
        time.sleep(API_SLEEP_SECONDS)
            
    write_log(f"--- {SCRIPT_NAME} 采集完成。---", "INFO")

if __name__ == '__main__':
    run_05_limit_price()