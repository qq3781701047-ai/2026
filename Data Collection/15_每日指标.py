import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import tushare as ts
from tqdm import tqdm 
import time

# -------------------------- T0: 元信息 (Meta Info) --------------------------
SCRIPT_NAME = "15_每日指标"
INTERFACE_NAME = "daily_basic"
PARTITION_KEY = 'trade_date' 

# 输出目录：D:\2025\raw\股票数据\行情数据\每日指标\
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '行情数据', '每日指标')
BASE_DIR = r"D:\2025"
# FIX-1: 明确定义最终输出目录
OUTPUT_DIR = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
# FIX-2: 输出文件格式改为 CSV
FILE_EXTENSION = '.csv' 

TODAY_DATE_STR = datetime.now().strftime('%Y%m%d')
FULL_START_DATE = '19901219'

# -------------------------- Tushare 配置与初始化 --------------------------
TS_TOKEN = "374f3850d22ecc9f328be05c8f655f274ed5ff0c2c8a45f39fd76092" 
try:
    ts.set_token(TS_TOKEN)
    PRO_API = ts.pro_api()
except Exception:
    print("FATAL ERROR: Tushare Token 配置失败或未配置。")
    sys.exit(1)

# -------------------------- T3: 字段契约 (Contract) --------------------------
OUTPUT_FIELDS = [
    'ts_code', 'trade_date', 'close', 'turnover_rate', 'turnover_rate_f', 
    'volume_ratio', 'pe', 'pe_ttm', 'pb', 'ps', 'ps_ttm', 'dv_ratio', 
    'dv_ttm', 'total_share', 'float_share', 'free_share', 'total_mv', 
    'circ_mv', 'limit_status', 'up_limit', 'down_limit'
]
PK_COLUMNS = ['ts_code', 'trade_date'] 

# -------------------------- T4: 日志和错误处理 --------------------------
def write_log(message, level="INFO"):
    """简易日志记录"""
    timestamp = datetime.now().strftime('[%Y-%m-%d %H:%M:%S]')
    log_message = f"{timestamp} [{level}] {message}"
    print(log_message)

def fatal_exit(message):
    """记录致命错误并退出"""
    write_log(f"FATAL EXIT: {message}", "ERROR")
    sys.exit(1)

# -------------------------- 增量采集和断点续采逻辑 --------------------------
def get_latest_local_date():
    """
    检查本地 CSV 分区目录，找到最新的 trade_date 分区。
    """
    if not os.path.exists(OUTPUT_DIR):
        write_log(f"本地目录 {OUTPUT_DIR} 不存在，将执行全量采集。", "INFO")
        return None

    # 查找所有 trade_date=YYYYMMDD 格式的文件夹
    partition_dirs = [d for d in os.listdir(OUTPUT_DIR) 
                      if os.path.isdir(os.path.join(OUTPUT_DIR, d)) and d.startswith(f"{PARTITION_KEY}=")]
    
    if not partition_dirs:
        write_log("本地目录为空，将执行全量采集。", "INFO")
        return None
        
    dates = []
    for d in partition_dirs:
        try:
            date_str = d.split('=')[-1]
            # 确保文件夹内有实际的数据文件（例如 CSV）
            file_path = os.path.join(OUTPUT_DIR, d, f"{SCRIPT_NAME}{FILE_EXTENSION}")
            if len(date_str) == 8 and os.path.exists(file_path):
                dates.append(date_str)
        except Exception:
            continue
    
    if not dates:
        write_log("未找到有效的本地分区日期文件，将执行全量采集。", "INFO")
        return None
        
    latest_date = max(dates)
    write_log(f"本地最新分区日期: {latest_date}", "INFO")
    return latest_date

# -------------------------- T5: 数据获取和 T7: 持久化（逐日） --------------------------
def get_trade_dates(start_date, end_date):
    """获取指定时间范围内的交易日历"""
    write_log(f"开始获取交易日历: {start_date} - {end_date}")
    try:
        df = PRO_API.trade_cal(exchange='', 
                               start_date=start_date, 
                               end_date=end_date, 
                               is_open='1')
        dates = df['cal_date'].tolist()
        write_log(f"获取到 {len(dates)} 个交易日。")
        return dates
    except Exception as e:
        fatal_exit(f"获取交易日历失败: {e}")

# -------------------------- T6: 数据清理与断言 --------------------------
def clean_and_assert(df):
    """执行基本清理和数据质量断言"""
    if df.empty:
        return df

    # T8: 数据清理 - 确保分区键是字符串格式
    df[PARTITION_KEY] = df[PARTITION_KEY].astype(str)
    
    # 检查核心字段的空值情况 (T9: 六断言 - 零缺失)
    if df['ts_code'].isnull().any() or df[PARTITION_KEY].isnull().any():
        fatal_exit("T9 断言失败: ts_code 或 trade_date 存在空值，违反零缺失原则。")
        
    # 去重处理 (T10: 幂等要求 - 确保只写最新版本)
    # 对于单日数据，此操作确保同一日期的同一股票不重复
    if df.duplicated(subset=PK_COLUMNS).any():
        # 如果单日内出现重复主键，应视为严重错误
        fatal_exit("T9 断言失败: 单日数据中存在重复主键，违反唯一性原则。")
        
    return df

def collect_and_save_date(date, fields):
    """
    获取单个日期的数据，清理并保存到 CSV 分区文件。
    该函数是实现断点续采的核心，失败不影响已保存的数据。
    """
    try:
        # 1. API Fetch
        df = PRO_API.daily_basic(
            trade_date=date,
            fields=','.join(fields)
        )
        
        if df.empty:
            write_log(f"日期 {date} 无数据返回，跳过。", "WARNING")
            return 0 
        
        # 2. Clean and Assert (T6)
        df = clean_and_assert(df)
        
        if df.empty:
            write_log(f"日期 {date} 清理后数据为空，跳过。", "WARNING")
            return 0
            
        # 3. Save to Disk (T7: Persistent Storage)
        partition_dir = os.path.join(OUTPUT_DIR, f"{PARTITION_KEY}={date}")
        os.makedirs(partition_dir, exist_ok=True)
        
        # FIX-2: 更改输出格式为 CSV
        output_path = os.path.join(partition_dir, f"{SCRIPT_NAME}{FILE_EXTENSION}")
        
        # 写入 CSV 文件 (成功写入覆盖既有文件 T10: 幂等)
        df.to_csv(output_path, index=False, encoding='utf-8')
        
        write_log(f"分区 {date} 成功保存 {len(df)} 行到 {output_path}", "DEBUG")
        return len(df)
        
    except Exception as e:
        # 记录错误，不致命退出，允许继续下一个日期 (错误容忍)
        write_log(f"处理 {date} 失败: {e}", "ERROR")
        return 0

# -------------------------- T1: 主逻辑运行 --------------------------
if __name__ == "__main__":
    write_log(f"--- {SCRIPT_NAME} 脚本启动 (支持断点续采/CSV) ---")
    
    # 1. 确定采集起始日期
    latest_local_date = get_latest_local_date()
    
    if latest_local_date:
        # 增量/续采模式：从本地最新日期的后一天开始采集
        start_dt = datetime.strptime(latest_local_date, '%Y%m%d') + timedelta(days=1)
        START_DATE = start_dt.strftime('%Y%m%d')
        
        if START_DATE > TODAY_DATE_STR:
            write_log("本地数据已是最新，无需采集。", "INFO")
            sys.exit(0)
            
    else:
        # 全量模式：首次运行或本地无数据
        START_DATE = FULL_START_DATE
        
    END_DATE = TODAY_DATE_STR
    
    write_log(f"实际采集范围: {START_DATE} - {END_DATE}")
    
    # 2. 获取需要采集的交易日列表
    trade_dates = get_trade_dates(START_DATE, END_DATE)
    
    if not trade_dates:
        write_log("未找到可采集的交易日，脚本结束。")
        sys.exit(0)
        
    # 3. 逐日采集并保存 (实现断点续采)
    total_rows = 0
    total_dates = len(trade_dates)
    
    # 使用 tqdm 来显示总体进度
    for i, date in enumerate(tqdm(trade_dates, desc=f"Collecting {SCRIPT_NAME}")):
        # 核心采集和保存逻辑
        rows = collect_and_save_date(date, OUTPUT_FIELDS)
        total_rows += rows

    write_log(f"全量采集/续采完成。总计处理日期: {total_dates}，总计保存行数: {total_rows}。")
    write_log(f"--- {SCRIPT_NAME} 脚本运行完毕 ---")