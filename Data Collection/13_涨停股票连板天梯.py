import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import tushare as ts
from tqdm import tqdm 
import time

# -------------------------- Tushare 配置 --------------------------
# !!! 注意: 需替换为您的 Tushare Token !!!
TS_TOKEN = "374f3850d22ecc9f328be05c8f655f274ed5ff0c2c8a45f39fd76092" 
try:
    ts.set_token(TS_TOKEN)
    PRO_API = ts.pro_api()
except Exception:
    print("FATAL ERROR: Tushare Token 配置失败或未配置。请检查 TS_TOKEN 变量。")
    sys.exit(1)
# ------------------------------------------------------------------

# --- 基础配置与路径 ---
SCRIPT_NAME = "13_涨停股票连板天梯"
BASE_DIR = r"D:\2025"
TODAY_DATE_STR = datetime.now().strftime('%Y%m%d')

# 输入文件路径
CALENDAR_PATH = os.path.join(BASE_DIR, 'raw', '股票数据', '基础数据', '交易日历', 'trade_calendar_full.csv')

# 输出目录配置
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '打板专题数据', '涨停股票连板天梯')
OUTPUT_DIR = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 确保日志目录存在
LOG_DIR = os.path.join(BASE_DIR, 'logs', datetime.now().strftime('%Y%m%d'))
os.makedirs(LOG_DIR, exist_ok=True) 

# 定义输出字段
OUTPUT_FIELDS = [
    'ts_code', 'name', 'trade_date', 'nums'
]

def write_log(message):
    """写入运行日志文件。"""
    log_file = os.path.join(LOG_DIR, f"{SCRIPT_NAME}_run.log")
    formatted_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(formatted_message)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(formatted_message + '\n')

def save_to_csv_partitioned(df_day, date_str, subdir):
    """数据落地函数，CSV 格式，按 trade_date 分区。"""
    partition_dir = os.path.join(BASE_DIR, 'raw', subdir, f"trade_date={date_str}")
    os.makedirs(partition_dir, exist_ok=True)
    output_path = os.path.join(partition_dir, 'data.csv')
    
    try:
        if df_day.empty:
            df_empty = pd.DataFrame(columns=OUTPUT_FIELDS)
            df_empty.to_csv(output_path, index=False, encoding='utf-8')
            return True
        
        # 确保字段顺序和内容正确
        df_day[OUTPUT_FIELDS].to_csv(output_path, index=False, encoding='utf-8')
        return True
    except Exception as e:
        write_log(f"数据落地失败 (CSV) on {date_str}: {e}")
        return False

def load_trade_dates():
    """加载并返回截止到今天的交易日历列表。"""
    if not os.path.exists(CALENDAR_PATH):
        write_log(f"FATAL ERROR: 交易日历文件不存在，请先运行 01_交易日历.py。")
        sys.exit(1)
        
    df_cal = pd.read_csv(CALENDAR_PATH, usecols=['cal_date', 'is_open', 'exchange'])
    trade_dates = df_cal[
        (pd.to_numeric(df_cal['is_open'], errors='coerce') == 1)
    ]['cal_date'].astype(str).tolist()
    
    # 限制交易日历的截止日期为今天
    return [d for d in trade_dates if d <= TODAY_DATE_STR]

def run_script():
    write_log(f"--- {SCRIPT_NAME}.py 开始执行 (采集连板天梯) ---")
    
    trade_dates = load_trade_dates()
    if not trade_dates: return

    # 连板数据历史较短，但我们从合理的较早日期开始拉取，并按天循环
    START_DATE = '20150101'
    trade_dates_to_pull = [d for d in trade_dates if d >= START_DATE]
    
    if not trade_dates_to_pull:
        write_log("没有需要采集的交易日。")
        return

    write_log(f"1. 准备按日期采集连板天梯数据。时间轴范围: {trade_dates_to_pull[0]} 到 {trade_dates_to_pull[-1]}。")

    # 按天调用 limit_step 接口获取全市场快照并分区落地
    for date_str in tqdm(trade_dates_to_pull, desc=f"分区落地 [{SCRIPT_NAME}]"):
        try:
            # 核心逻辑：按日期调用 API 获取当日连板数据
            df_data = PRO_API.limit_step(trade_date=date_str)
            
            # 引入延时机制
            time.sleep(0.1) 
            
            if df_data.empty:
                continue 
                
            # 清理字段，保留所需的输出字段
            df_data = df_data[OUTPUT_FIELDS]
            
            # 步骤 3: 落地数据
            save_to_csv_partitioned(df_data, date_str, OUTPUT_BASE_SUBDIR)
            
        except Exception as e:
            write_log(f"采集/落地失败 on {date_str}: {e}")
            # sys.exit(1)
            
    write_log(f"--- {SCRIPT_NAME} 采集完成。---")

if __name__ == '__main__':
    run_script()