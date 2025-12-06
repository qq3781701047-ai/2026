import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import tushare as ts
from tqdm import tqdm 
import glob
import time

# -------------------------- Tushare 配置 --------------------------
# !!! 替换为您的 Tushare Token !!!
TS_TOKEN = "374f3850d22ecc9f328be05c8f655f274ed5ff0c2c8a45f39fd76092" 
try:
    ts.set_token(TS_TOKEN)
    PRO_API = ts.pro_api()
except Exception:
    print("FATAL ERROR: Tushare Token 配置失败或未配置。请检查 TS_TOKEN 变量。")
    sys.exit(1)
# ------------------------------------------------------------------

# --- 基础配置与通用函数 ---
SCRIPT_NAME = "07_股票历史列表"
BASE_DIR = r"D:\2025"
TODAY_DATE = datetime.now()
TODAY_DATE_STR = TODAY_DATE.strftime('%Y%m%d')
LOG_DIR = os.path.join(BASE_DIR, 'logs', TODAY_DATE_STR)
os.makedirs(LOG_DIR, exist_ok=True) 

# 目标目录结构：按 trade_date 分区
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '基础数据', '股票历史列表')
OUTPUT_DIR = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 交易日历文件的位置，用于确定时间轴
CALENDAR_PATH = os.path.join(BASE_DIR, 'raw', '股票数据', '基础数据', '交易日历', 'trade_calendar_full.csv')

# bak_basic 接口的数据起始日期为 2016 年
MIN_START_DATE = '20160101' 

# 最终需要的字段契约 (bak_basic 接口返回的字段)
FIELD_CONTRACT = [
    'trade_date', 'ts_code', 'name', 'industry', 'area', 'pe', 'float_share', 
    'total_share', 'total_assets', 'liquid_assets', 'fixed_assets', 'reserved', 
    'reserved_pershare', 'eps', 'bvps', 'pb', 'list_date', 'undp', 
    'per_undp', 'rev_yoy', 'profit_yoy', 'gpr', 'npr', 'holder_num'
]

# ************* 最终调整：最保守延时 60.0 秒 *************
# 权限限制：每分钟最多 2 次。本次调用间隔设置为 60.0 秒，确保每分钟仅调用 1 次。
API_CALL_INTERVAL_SECONDS = 60.0 
# **********************************************

def write_log(message):
    """写入运行日志文件。"""
    log_file = os.path.join(LOG_DIR, f"{SCRIPT_NAME}_run.log")
    formatted_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(formatted_message)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(formatted_message + '\n')

def fetch_trade_dates():
    """读取 01_交易日历 的产物，获取全市场的交易日列表。"""
    write_log(f"检查交易日历文件是否存在: {CALENDAR_PATH}")
    if not os.path.exists(CALENDAR_PATH):
        write_log(f"FATAL ERROR: 交易日历文件不存在，请先运行 01_交易日历.py。")
        sys.exit(1)
    
    try:
        # 只读取 SSE 的开市日作为采集时间轴
        df_cal = pd.read_csv(CALENDAR_PATH, usecols=['cal_date', 'is_open', 'exchange'])
        df_trade_dates = df_cal[
            (df_cal['exchange'] == 'SSE') & 
            (pd.to_numeric(df_cal['is_open'], errors='coerce') == 1)
        ]['cal_date'].astype(str).tolist() 
        
        # 过滤到 bak_basic 接口的起始日期
        return sorted([d for d in df_trade_dates if d >= MIN_START_DATE])
        
    except Exception as e:
        write_log(f"读取或解析交易日历文件失败: {e}。")
        sys.exit(1)

def get_last_collected_date(output_dir):
    """断点续采：获取已落地数据的最新分区日期 (按 trade_date=...)。"""
    partition_dirs = glob.glob(os.path.join(output_dir, "trade_date=*"))
    
    if not partition_dirs:
        # 返回最早日期前一天
        start_dt = datetime.strptime(MIN_START_DATE, '%Y%m%d') - timedelta(days=1)
        return start_dt.strftime('%Y%m%d')
    
    dates = [os.path.basename(d).split('=')[-1] for d in partition_dirs]
    latest_date = max(dates)
    
    # 确保返回的日期有效且不超前
    if latest_date < MIN_START_DATE or latest_date > TODAY_DATE_STR:
        return datetime.strptime(MIN_START_DATE, '%Y%m%d').strftime('%Y%m%d')

    return latest_date

def save_to_csv_partitioned(df_day, date_str, subdir):
    """数据落地函数，CSV 格式，按 trade_date 分区。"""
    output_base_dir = os.path.join(BASE_DIR, 'raw', subdir)
    
    # 分区目录结构：D:\2025\raw\股票数据\基础数据\股票历史列表\trade_date=YYYYMMDD\data.csv
    partition_dir = os.path.join(output_base_dir, f"trade_date={date_str}")
    os.makedirs(partition_dir, exist_ok=True)
    
    output_path = os.path.join(partition_dir, 'data.csv')
    
    try:
        if df_day.empty:
            return False
            
        # 严格遵守字段契约和顺序
        cols_to_save = [col for col in FIELD_CONTRACT if col in df_day.columns]
        
        if not all(col in df_day.columns for col in FIELD_CONTRACT):
            write_log(f"警告: {date_str} 的 API 返回字段可能不完整。")

        df_day[cols_to_save].to_csv(output_path, index=False, encoding='utf-8')
        return True
    except Exception as e:
        write_log(f"数据落地失败 (CSV) on {date_str}: {e}")
        return False

def run_07_bak_basic():
    write_log(f"--- {SCRIPT_NAME}.py 开始执行 (V07 - 按日期分区) ---")
    
    # 步骤 1: 获取交易日历（时间轴）
    all_trade_dates = fetch_trade_dates()
    
    # 步骤 2: 确定断点续采的起始日期
    last_collected_date = get_last_collected_date(OUTPUT_DIR)
    
    start_date_dt = datetime.strptime(last_collected_date, '%Y%m%d') + timedelta(days=1)
    start_date_str = start_date_dt.strftime('%Y%m%d')
    
    # 过滤交易日历，只保留需要采集的日期
    trade_dates_to_pull = [d for d in all_trade_dates if d >= start_date_str and d <= TODAY_DATE_STR]

    write_log(f"增量采集：从 {start_date_str} 开始获取股票历史列表 (bak_basic)。")
    write_log(f"API 频率限制：每分钟最多 2 次。本次调用间隔设置为 {API_CALL_INTERVAL_SECONDS} 秒。")
    write_log(f"待采集交易日数量: {len(trade_dates_to_pull)}") 
    if not trade_dates_to_pull:
        write_log("没有需要采集的新交易日。")
        return
        
    # --- 3. 循环交易日，按天调用 API 获取全市场快照并分区落地 ---
    
    write_log(f"开始生成并分区落地 {len(trade_dates_to_pull)} 天的快照 (按天调用 bak_basic 接口)...")

    for date_str in tqdm(trade_dates_to_pull, desc=f"分区落地 [{SCRIPT_NAME}]"):
        
        # 核心逻辑：按日期调用 API 获取当日全市场数据
        try:
            # bak_basic 接口：拉取某一天的全市场基础列表数据
            df_basic = PRO_API.bak_basic(trade_date=date_str)
            
            # ** 引入长延时机制 **
            # 延时必须在 API 调用之后执行，以等待下一个窗口期
            time.sleep(API_CALL_INTERVAL_SECONDS)
            
            if df_basic.empty:
                continue 
                
            # 步骤 4: 落地数据
            if save_to_csv_partitioned(df_basic, date_str, OUTPUT_BASE_SUBDIR):
                pass
                
        except Exception as e:
            # 捕获所有已知的频率限制错误，并记录
            if "daily limit" in str(e).lower() or "frequency limit" in str(e).lower() or "最多访问该接口" in str(e):
                # 记录当前的 date_str，下次运行将从这里断点续传
                write_log(f"API 调用频率限制警告！已停止采集。下次运行将从 {date_str} 处断点续传。")
            else:
                write_log(f"致命错误：采集 {date_str} 失败: {e}")
                
            # 停止脚本
            sys.exit(1)
            
    write_log(f"--- {SCRIPT_NAME} 采集完成。---")

if __name__ == '__main__':
    run_07_bak_basic()