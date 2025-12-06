import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import tushare as ts
from tqdm import tqdm 
import time

# -------------------------- Tushare 配置 (T0: 元信息) --------------------------
# !!! 注意: 必须替换为您的 Tushare Token !!!
TS_TOKEN = "374f3850d22ecc9f328be05c8f655f274ed5ff0c2c8a45f39fd76092" 
try:
    ts.set_token(TS_TOKEN)
    PRO_API = ts.pro_api()
except Exception:
    print("FATAL ERROR: Tushare Token 配置失败或未配置。")
    sys.exit(1)
# --------------------------------------------------------------------------------

BASE_DIR = r"D:\2025" 
TODAY_DATE_STR = datetime.now().strftime('%Y%m%d')
MAX_RETRY = 3 
# T6: 0.5秒等待，满足 Tushare 200次/分钟的限制
API_SLEEP_SECONDS = 0.5 
LOG_DIR = os.path.join(BASE_DIR, 'logs', TODAY_DATE_STR)
os.makedirs(LOG_DIR, exist_ok=True)


# -------------------------- T6/T7/T11/T10: 核心工程函数 --------------------------

def write_log(script_name, message, level="INFO"):
    """写入运行日志文件 (T7: 进度条与日志)。"""
    log_file = os.path.join(LOG_DIR, f"{script_name}_run.log")
    formatted_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [{level}] {message}"
    print(formatted_message) 
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(formatted_message + '\n')

def fatal_exit(script_name, reason):
    """硬停止函数 (T11: 上下游影响, T9/T8 断言失败)。"""
    write_log(script_name, f"HARD STOP: {reason}", level="ERROR")
    sys.exit(1)

def save_data_by_partition(script_name, df, output_base_subdir, partition_key, pk_columns):
    """
    T10 幂等保存函数：将单日数据保存为 CSV 格式，并立即保存到磁盘。
    """
    if df.empty:
        write_log(script_name, "当前日期数据为空，跳过保存。", "DEBUG")
        return

    # 验证 df 确实是单个分区（日期）
    if len(df[partition_key].unique()) > 1:
        fatal_exit(script_name, "保存函数错误: 传入了超过一个分区键的值，必须是单日数据。")

    partition_value = df[partition_key].iloc[0]
    output_dir = os.path.join(BASE_DIR, 'raw', output_base_subdir)
    
    # T9/T8: 六断言 - 检查重复主键（这是最后一道防线）
    if df.duplicated(subset=pk_columns).any():
        fatal_exit(script_name, f"T9/T8 断言失败: 日期 {partition_value} 存在重复主键。")

    # 构造分区路径
    partition_dir = os.path.join(output_dir, f"{partition_key}={partition_value}")
    os.makedirs(partition_dir, exist_ok=True)
    
    # 输出文件为 .csv 格式
    output_path = os.path.join(partition_dir, f"{script_name}.csv") 
    
    # 成功写入覆盖既有文件 (T10: 幂等)
    try:
        # 使用 utf-8-sig 编码确保中文在 Excel 中打开时不乱码
        df.to_csv(output_path, index=False, encoding='utf-8-sig') 
        write_log(script_name, f"分区 {partition_value} 成功保存 {len(df)} 行到 CSV。", "INFO") 
    except Exception as e:
        fatal_exit(script_name, f"保存分区 {partition_value} 失败: {e}")

def get_date_range(script_name, start_date_str, end_date_str):
    """获取交易日列表，确保只认 real_date。"""
    try:
        # 此函数处理从 start_date_str 到 end_date_str 之间的所有交易日
        df_cal = PRO_API.trade_cal(exchange='', start_date=start_date_str, end_date=end_date_str)
        return [str(d) for d in df_cal[df_cal['is_open'] == 1]['cal_date'].tolist()]
    except Exception as e:
        fatal_exit(script_name, f"获取交易日历失败: {e}")

# -------------------------- T1/T2/T10: 增量/断点续采核心函数 --------------------------

def get_last_collected_date(output_base_subdir, partition_key):
    """根据本地目录结构，查找最后一次成功采集的日期。"""
    full_path = os.path.join(BASE_DIR, 'raw', output_base_subdir)
    if not os.path.exists(full_path):
        return None
    
    try:
        partition_dirs = [d for d in os.listdir(full_path) 
                          if os.path.isdir(os.path.join(full_path, d)) and d.startswith(f"{partition_key}=")]

        if not partition_dirs:
            return None

        dates = [d.split('=')[1] for d in partition_dirs if len(d.split('=')) == 2 and d.split('=')[1].isdigit()]
        if not dates: return None
        
        latest_date_str = max(dates)
        return latest_date_str

    except Exception as e:
        write_log("INCREMENTAL_HELPER", f"读取本地分区失败: {e}，将执行全量采集。", "ERROR")
        return None

def get_incremental_start_date(script_name, output_base_subdir, partition_key, full_load_start="20050101"):
    """
    确定本次采集的起始日期（上次采集日期的下一个日历日，修复了跳过中间日期的 Bug）。
    """
    last_date_str = get_last_collected_date(output_base_subdir, partition_key)

    if last_date_str is None:
        write_log(script_name, f"未找到本地分区，执行全量采集。起始日期: {full_load_start}")
        return full_load_start
    
    try:
        # --- 核心修复逻辑：只计算下一个日历日 ---
        next_day = datetime.strptime(last_date_str, '%Y%m%d') + timedelta(days=1)
        next_start_date_str = next_day.strftime('%Y%m%d')
        
        if next_start_date_str > TODAY_DATE_STR:
             write_log(script_name, f"上次采集日 {last_date_str} 之后没有新的日期，增量采集完成。")
             return None

        # 此次返回的是下一个日历日，让 run_collection_loop 去查询该范围内的所有交易日。
        write_log(script_name, f"启用增量采集。上次采集日: {last_date_str}，本次起始日: {next_start_date_str}")
        return next_start_date_str

    except Exception as e:
        fatal_exit(script_name, f"确定增量起始日期失败: {e}")


# -------------------------- SCRIPT 09: 龙虎榜每日统计单 --------------------------

# -------------------------- T0/T3: 元信息与契约 (已修正) --------------------------
SCRIPT_NAME = "09_龙虎榜每日统计单"
INTERFACE_NAME = "top_list" 
PARTITION_KEY = 'trade_date' 
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '打板专题数据', '龙虎榜每日统计单') 
FULL_LOAD_START_DATE = "20050101" 
OUTPUT_FIELDS = [
    'trade_date', 'ts_code', 'name', 'close', 'pct_change', 
    'turnover_rate', 'amount', 'l_sell', 'l_buy', 'l_amount', 
    'net_amount', 'net_rate', 'amount_rate', 'float_values', 'reason'
]
PK_COLUMNS = ['trade_date', 'ts_code', 'reason'] 

# -------------------------- 核心采集/保存循环 (断点续采实现) --------------------------
def run_collection_loop(start_date, end_date):
    """执行按日期循环采集，并立即保存到磁盘 (断点续采)。"""
    
    # get_date_range 将返回 start_date 到 end_date 之间的所有交易日，包括您删除的中间日期
    trade_dates = get_date_range(SCRIPT_NAME, start_date, end_date)
    total_dates = len(trade_dates)
    
    for date_str in tqdm(trade_dates, desc=f"Fetching & Saving {INTERFACE_NAME}"):
        df_day = None
        
        # 内部重试循环
        for attempt in range(MAX_RETRY):
            try:
                # 1. API 调用
                df_day = PRO_API.top_list(
                    trade_date=date_str, 
                    fields=','.join(OUTPUT_FIELDS)
                )
                
                # 2. 如果成功，跳出重试
                if df_day is not None:
                    break 
            
            except Exception as e:
                if attempt < MAX_RETRY - 1:
                    write_log(SCRIPT_NAME, f"API 失败，重试 {attempt + 1}/{MAX_RETRY} on {date_str}: {e}", "WARNING")
                    time.sleep(API_SLEEP_SECONDS * (2 ** attempt)) # T6: 指数退避
                else:
                    write_log(SCRIPT_NAME, f"日期 {date_str} API 最终失败，跳过采集/保存。", "ERROR")
                    df_day = pd.DataFrame() 
                    break 
            
            time.sleep(API_SLEEP_SECONDS)

        # 3. 立即保存当前日期的采集结果（断点续采的核心）
        if df_day is not None:
            df_day = df_day.rename(columns={'trade_date': PARTITION_KEY})
            
            # --- T3/T8 字段契约与主键鲁棒性修正 ---
            missing_output_cols = set(OUTPUT_FIELDS) - set(df_day.columns)
            if missing_output_cols:
                 for col in missing_output_cols:
                    if col in PK_COLUMNS:
                        write_log(SCRIPT_NAME, f"日期 {date_str} 缺失关键主键列 '{col}'。用 None 填充。", "WARNING")
                    df_day[col] = None
                    
            df_day = df_day[OUTPUT_FIELDS]
            
            # --- T9/T8 预处理（强制去重） ---
            initial_rows = len(df_day)
            df_day.drop_duplicates(subset=PK_COLUMNS, keep='first', inplace=True) 
            if len(df_day) < initial_rows:
                write_log(SCRIPT_NAME, f"日期 {date_str} 通过 drop_duplicates 移除了 {initial_rows - len(df_day)} 行重复记录。", "WARNING")
            # -------------------------------------------------------------
            
            save_data_by_partition(
                SCRIPT_NAME, 
                df_day, 
                OUTPUT_BASE_SUBDIR, 
                PARTITION_KEY, 
                PK_COLUMNS
            )
        
    write_log(SCRIPT_NAME, f"--- 采集循环完成。共处理 {total_dates} 个交易日。---")


# -------------------------- T1: 主逻辑运行 --------------------------
if __name__ == "__main__":
    
    # T1 逻辑：使用增量函数确定起始日期（下一个日历日）
    _start_date = get_incremental_start_date(
        SCRIPT_NAME, 
        OUTPUT_BASE_SUBDIR, 
        PARTITION_KEY,
        full_load_start=FULL_LOAD_START_DATE
    )
    _end_date = TODAY_DATE_STR
    
    if _start_date is None:
        write_log(SCRIPT_NAME, f"--- 脚本退出 (无增量数据) ---")
        sys.exit(0)

    write_log(SCRIPT_NAME, f"--- 脚本启动 (启用断点续采/增量模式) ---")
    write_log(SCRIPT_NAME, f"本次采集范围: {_start_date} - {_end_date}")

    # run_collection_loop 会负责查询这个日历范围内的所有交易日，并进行采集
    run_collection_loop(_start_date, _end_date)
    
    write_log(SCRIPT_NAME, f"--- 脚本完成 ---")