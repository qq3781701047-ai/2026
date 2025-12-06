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
# !!! 注意: 需与 01/02/03 脚本使用相同的真实 Tushare Token !!!
TS_TOKEN = "374f3850d22ecc9f328be05c8f655f274ed5ff0c2c8a45f39fd76092" 
try:
    ts.set_token(TS_TOKEN)
    PRO_API = ts.pro_api()
except Exception:
    print("FATAL ERROR: Tushare Token 配置失败或未配置。请检查 TS_TOKEN 变量。")
    sys.exit(1)
# ---------------------------------------------------------------------------------------

# --- 基础配置与通用函数 ---
BASE_DIR = r"D:\2025"
TODAY_DATE = datetime.now()
TODAY_DATE_STR = TODAY_DATE.strftime('%Y%m%d')
LOG_DIR = os.path.join(BASE_DIR, 'logs', TODAY_DATE_STR)
os.makedirs(LOG_DIR, exist_ok=True) 

# 目标目录结构：按 trade_date 分区
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '行情数据', '历史日线')
OUTPUT_DIR = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 交易日历文件的位置，用于确定时间轴
CALENDAR_PATH = os.path.join(BASE_DIR, 'raw', '股票数据', '基础数据', '交易日历', 'trade_calendar_full.csv')

# 最终需要的字段契约
FIELD_CONTRACT = [
    'ts_code', 'trade_date', 'open', 'high', 'low', 'close', 
    'pre_close', 'change', 'pct_chg', 'vol', 'amount'
]
MIN_START_DATE = '19901219' 

def write_log(message, script_name):
    """写入运行日志文件。"""
    log_file = os.path.join(LOG_DIR, f"{script_name}_run.log")
    formatted_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(formatted_message)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(formatted_message + '\n')

def fetch_trade_dates():
    """读取 01_交易日历 的产物，获取全市场的交易日列表。"""
    script_name = "04_历史日线"
    write_log(f"检查交易日历文件是否存在: {CALENDAR_PATH}", script_name)
    if not os.path.exists(CALENDAR_PATH):
        write_log(f"FATAL ERROR: 交易日历文件不存在，请先运行 01_交易日历.py。", script_name)
        sys.exit(1)
    
    try:
        df_cal = pd.read_csv(CALENDAR_PATH, usecols=['cal_date', 'is_open', 'exchange'])
        
        # 筛选上海交易所 (SSE) 的有效交易日，并确保是开放日
        df_trade_dates = df_cal[
            (df_cal['exchange'] == 'SSE') & 
            (pd.to_numeric(df_cal['is_open'], errors='coerce') == 1)
        ]['cal_date'].astype(str).tolist() 
        
        # 仅保留从 A 股开市日开始的交易日
        return sorted([d for d in df_trade_dates if d >= MIN_START_DATE])
        
    except Exception as e:
        write_log(f"读取或解析交易日历文件失败: {e}。", script_name)
        sys.exit(1)

def get_last_collected_date(output_dir):
    """断点续采：获取已落地数据的最新分区日期 (按 trade_date=...)。"""
    partition_dirs = glob.glob(os.path.join(output_dir, "trade_date=*"))
    
    if not partition_dirs:
        # 如果文件不存在，返回最早日期前一天
        return '19901218' 
    
    dates = [os.path.basename(d).split('=')[-1] for d in partition_dirs]
    latest_date = max(dates)
    
    if latest_date < MIN_START_DATE or latest_date > TODAY_DATE_STR:
        return '19901218' 

    return latest_date

def save_to_csv_partitioned(df_day, date_str, subdir, script_name):
    """数据落地函数，CSV 格式，按 trade_date 分区。"""
    output_base_dir = os.path.join(BASE_DIR, 'raw', subdir)
    
    # 分区目录结构：D:\2025\raw\股票数据\行情数据\历史日线\trade_date=20251111\data.csv
    partition_dir = os.path.join(output_base_dir, f"trade_date={date_str}")
    os.makedirs(partition_dir, exist_ok=True)
    
    output_path = os.path.join(partition_dir, 'data.csv')
    
    try:
        # 确保数据不为空且包含所需字段
        if df_day.empty:
            return False
            
        # 严格遵守字段契约和顺序
        df_day[FIELD_CONTRACT].to_csv(output_path, index=False, encoding='utf-8')
        return True
    except Exception as e:
        write_log(f"数据落地失败 (CSV) on {date_str}: {e}", script_name)
        return False

def run_04_daily():
    script_name = "04_历史日线（不复权）"
    write_log(f"--- {script_name}.py 开始执行 (V05 - 按日期分区) ---", script_name)
    
    # 步骤 1: 获取交易日历（时间轴）
    all_trade_dates = fetch_trade_dates()
    
    # 步骤 2: 确定断点续采的起始日期
    last_collected_date = get_last_collected_date(OUTPUT_DIR)
    
    start_date_dt = datetime.strptime(last_collected_date, '%Y%m%d') + timedelta(days=1)
    start_date_str = start_date_dt.strftime('%Y%m%d')
    
    # 过滤交易日历，只保留需要采集的日期
    trade_dates_to_pull = [d for d in all_trade_dates if d >= start_date_str and d <= TODAY_DATE_STR]

    # ********************* 错误修正区域 *********************
    write_log(f"!!警告!! 切换为按日期采集，预计总 API 调用次数将大幅增加。", script_name) # 修正
    write_log(f"断点续采/增量采集：从 {start_date_str} 开始计算每日快照。", script_name)
    # ******************************************************
    
    write_log(f"待采集交易日数量: {len(trade_dates_to_pull)}", script_name) 
    if not trade_dates_to_pull:
        write_log("没有需要采集的新交易日。", script_name)
        return
        
    # --- 3. 循环交易日，按天调用 API 获取全市场快照并分区落地 ---
    
    write_log(f"开始生成并分区落地 {len(trade_dates_to_pull)} 天的快照 (按天调用 daily 接口)...", script_name)

    # 使用进度条封装交易日循环
    # 注意：此循环中调用 API，可能会触发 Tushare 的 500次/分钟限制！
    for date_str in tqdm(trade_dates_to_pull, desc=f"分区落地 [{script_name}]"):
        
        # 核心逻辑：按日期调用 API 获取当日全市场数据
        try:
            df_daily = PRO_API.daily(trade_date=date_str)
            
            # ** 引入延时机制，确保调用频率低于 500次/分钟 **
            time.sleep(0.15) 
            
            if df_daily.empty:
                continue # 当天可能没有股票上市，或数据缺失
                
            # 步骤 4: 落地数据
            # 修正：save_to_csv_partitioned 函数也需要 script_name 参数
            save_to_csv_partitioned(df_daily, date_str, OUTPUT_BASE_SUBDIR, script_name)
                
        except Exception as e:
            # 当遇到 API 限制或权限问题时，记录并停止，以便下次运行增量
            if "daily limit" in str(e).lower():
                write_log(f"API 调用频率限制警告！已停止采集。下次运行将从 {date_str} 处断点续传。", script_name)
            else:
                write_log(f"致命错误：采集 {date_str} 失败: {e}", script_name)
                
            # 遇到任何错误都停止，以便用户检查 Token 或等待频率恢复
            sys.exit(1)
            
    write_log(f"--- {script_name} 历史快照生成完成。---", script_name)

if __name__ == '__main__':
    run_04_daily()