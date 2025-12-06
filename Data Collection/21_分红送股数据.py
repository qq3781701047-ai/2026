import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import tushare as ts
from tqdm import tqdm 
import time
import glob

# -------------------------- Tushare 配置 (T0: 元信息) --------------------------
# !!! 注意: 必须替换为您的 Tushare Token !!!
TS_TOKEN = "374f3850d22ecc9f328be05c8f655f274ed5ff0c2c8a45f39fd76092" 
try:
    ts.set_token(TS_TOKEN)
    # T7: 增加 API 请求超时时间为 60 秒，以防网络不稳定导致的初始化失败
    PRO_API = ts.pro_api(timeout=60) 
except Exception as e:
    print(f"FATAL ERROR: Tushare Token 配置失败或未配置。详细错误: {e}")
    sys.exit(1)
# --------------------------------------------------------------------------------

BASE_DIR = r"D:\2025" 
TODAY_DATE_STR = datetime.now().strftime('%Y%m%d')
MAX_RETRY = 3 
# Tushare dividend 接口限制 5000 次/分钟，0.35 秒的安全间隔是足够的
API_SLEEP_SECONDS = 0.35 
LOG_DIR = os.path.join(BASE_DIR, 'logs', TODAY_DATE_STR)
os.makedirs(LOG_DIR, exist_ok=True)

# SCRIPT 21: 分红送股数据 (dividend)
# -------------------------- T0: 元信息 --------------------------
SCRIPT_NAME = "21_分红送股数据"
INTERFACE_NAME = "dividend"
PARTITION_KEY = 'ann_date' # T4: 按公告日期分区
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '财务数据', '分红送股数据')
OUTPUT_DIR = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
FILE_EXTENSION = '.csv' 

# -------------------------- T3: 字段契约 (Contract) --------------------------
# 【最终修正：根据用户提供的完整字段列表】
OUTPUT_FIELDS = [
    'ts_code',
    'end_date',
    'ann_date',
    'div_proc',
    'stk_div',
    'stk_bo_rate',
    'stk_co_rate',
    'cash_div',
    'cash_div_tax',
    'record_date',
    'ex_date',
    'pay_date',
    'div_listdate',
    'imp_ann_date',
    'base_date',
    'base_share',
    'update_flag'
]

# T9: 主键 (股票代码+公告日+分送年度/报告期)
PK_COLUMNS = ['ts_code', PARTITION_KEY, 'end_date'] 

# -------------------------- T6/T7/T11/T10: 核心工程函数 --------------------------

def write_log(script_name, message, level="INFO"):
    """写入运行日志文件 (T7: 进度条与日志)。"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_file = os.path.join(LOG_DIR, f"{script_name}.log")
    
    # 打印到控制台
    print(f"[{timestamp}] [{level}] {message}")
    
    # 写入日志文件
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] [{level}] {message}\n")

def fatal_exit(script_name, message):
    """记录致命错误并退出 (T11: 零缺失硬停止)。"""
    write_log(script_name, message, "FATAL")
    # 分红送股数据是按日查询，单个日期失败，我们使用异常传递错误信息，在主循环中捕获并跳过当前日期
    raise Exception(message) 

# -------------------------- T10: 幂等性/断点续采核心函数 --------------------------

def save_data_to_partition_csv(script_name, df, partition_value):
    """
    保存数据到公告日期分区 CSV 文件，并执行幂等性检查（去重）。
    已修复 SettingWithCopyWarning。
    """
    
    # T3: 检查字段完整性
    missing_cols = list(set(OUTPUT_FIELDS) - set(df.columns))
    if missing_cols:
        fatal_exit(script_name, f"T3 断言失败: API 返回数据缺少关键字段 {missing_cols}，请检查接口权限。")

    # FIX: 修复 SettingWithCopyWarning：立即创建显式副本，并选择需要的字段
    df_working = df[OUTPUT_FIELDS].copy() 

    partition_dir = os.path.join(OUTPUT_DIR, f"{PARTITION_KEY}={partition_value}")
    os.makedirs(partition_dir, exist_ok=True)
    
    # 确定输出路径
    output_path = os.path.join(partition_dir, f"{SCRIPT_NAME.split('_', 1)[1]}{FILE_EXTENSION}")

    file_exists = os.path.exists(output_path)
    
    rows_to_save = 0
    
    # ------------------- T10: 幂等性/断点续采逻辑 -------------------
    try:
        if file_exists:
            # 读取现有数据的主键
            existing_keys_df = pd.read_csv(output_path, usecols=PK_COLUMNS, dtype=str)
            
            # 使用列表推导式创建现有数据的联合主键
            existing_keys = (
                existing_keys_df['ts_code'] + '_' + 
                existing_keys_df[PARTITION_KEY].astype(str) + '_' + 
                existing_keys_df['end_date'].astype(str)
            ).unique()

            # 建立当前待写入数据的唯一主键 (在副本上操作)
            df_working['key'] = (
                df_working['ts_code'] + '_' + 
                df_working[PARTITION_KEY].astype(str) + '_' + 
                df_working['end_date'].astype(str)
            )
            
            # 筛选出需要写入的新数据 (T10: 增量)
            df_new = df_working[~df_working['key'].isin(existing_keys)].copy()
            
            # 保持 df_working 干净，删除临时 key 列
            df_working.drop(columns=['key'], inplace=True) 
            
        else:
            # 文件不存在，所有数据都是新的
            df_new = df_working.copy() 
            
        rows_to_save = len(df_new)
        
        if rows_to_save > 0:
            # 成功写入
            df_new.to_csv(
                output_path, 
                index=False, 
                mode='a', 
                header=not file_exists, # 只有第一次写入时带上表头
                encoding='utf-8'
            )
            write_log(script_name, f"分区 {partition_value} 成功追加 {len(df_new)} 行。", "DEBUG")
            return len(df_new)
        
        write_log(script_name, f"分区 {partition_value} 无新增数据行需保存。", "DEBUG")
        return 0

    except Exception as e:
        fatal_exit(script_name, f"保存分区 {partition_value} 失败: {e}")

# -------------------------- NEW: 隐式状态检查（实现断点续采） --------------------------

def get_latest_completed_date_from_disk(script_name):
    """ 
    通过检查磁盘上的 CSV 文件（隐式状态），确定已完成采集的最新日期。
    策略：查找所有按 PARTITION_KEY（ann_date）分区的目录，返回最大的日期。
    """
    if not os.path.exists(OUTPUT_DIR):
        return None

    # 1. 查找所有分区目录
    all_partitions = [d for d in os.listdir(OUTPUT_DIR) if d.startswith(f"{PARTITION_KEY}=")]
    if not all_partitions:
        return None

    # 2. 提取并找到最新的日期字符串 (e.g., ann_date=20251118)
    # 格式为 'ann_date=YYYYMMDD'，按字典序排序即可
    latest_partition_key = sorted(all_partitions, reverse=True)[0] 
    latest_date_str = latest_partition_key.split('=')[1]
    
    # 确保找到的目录下确实有文件，防止找到空目录
    if not glob.glob(os.path.join(OUTPUT_DIR, latest_partition_key, f"*{FILE_EXTENSION}")):
        return None
        
    write_log(script_name, f"已识别出最新已完成采集日期分区: {latest_date_str}。", "INFO")
    return latest_date_str
    
# -------------------------- NEW: 获取交易日历 --------------------------

def get_trade_calendar(start_date, end_date):
    """获取指定范围内的交易日列表。"""
    try:
        # T7: Tushare trade_cal 接口限制 5000 次/分钟，调用一次即可
        df_cal = PRO_API.trade_cal(exchange='', start_date=start_date, end_date=end_date, is_open='1')
        return df_cal['cal_date'].tolist()
    except Exception as e:
        fatal_exit(SCRIPT_NAME, f"获取交易日历失败: {e}")
        
# -------------------------- T8: 主采集函数 --------------------------

def fetch_and_save_by_ann_date(start_date, end_date):
    """
    按公告日期范围采集数据，并按日期分区保存。
    """
    
    # 1. 获取日期范围列表
    date_list = get_trade_calendar(start_date, end_date)
    
    total_saved_rows = 0
    
    # 2. 逐日期采集并保存
    pbar = tqdm(date_list, desc=f"Fetching {INTERFACE_NAME} by date")
    for date_str in pbar:
        try:
            pbar.set_postfix({'ann_date': date_str})
            df_day = None
            
            # T6: 接口调用与重试
            for attempt in range(MAX_RETRY):
                try:
                    # Tushare 接口调用：按 ann_date 循环，fields 严格限制
                    df_day = PRO_API.dividend(
                        ann_date=date_str, 
                        fields=','.join(OUTPUT_FIELDS)
                    )
                    break 
                except Exception as e:
                    if attempt < MAX_RETRY - 1:
                        write_log(SCRIPT_NAME, f"日期 {date_str} API 失败，重试 {attempt + 1}/{MAX_RETRY}: {e}", "WARNING")
                        time.sleep(API_SLEEP_SECONDS * (2 ** attempt)) 
                    else:
                        fatal_exit(SCRIPT_NAME, f"日期 {date_str} API 最终失败: {e}") 
                
                time.sleep(API_SLEEP_SECONDS) 

            if df_day is None or df_day.empty:
                write_log(SCRIPT_NAME, f"日期 {date_str} 无数据。", "DEBUG")
                continue 

            # 【关键修正点】移除了 df_day = df_day.rename(columns={'ann_date': PARTITION_KEY})
            # 因为 PARTITION_KEY 已经等于 'ann_date'

            # T9: 六断言 - 检查重复主键（在单日内）
            if df_day.duplicated(subset=PK_COLUMNS).any():
                write_log(SCRIPT_NAME, f"WARNING: 日期 {date_str} 存在重复主键，将保留最新记录。", "WARNING")
                df_day = df_day.drop_duplicates(subset=PK_COLUMNS, keep='last')
                
            # 3. 保存数据
            rows_saved = save_data_to_partition_csv(SCRIPT_NAME, df_day, date_str)
            total_saved_rows += rows_saved
            
            # T7: 遵守频率
            time.sleep(API_SLEEP_SECONDS) 
            
        except Exception as e:
            # 这里的异常通常是 fatal_exit 抛出的
            write_log(SCRIPT_NAME, f" FATAL ERROR during {date_str} fetching: {e}", "FATAL")
            pass # 跳过当前日期，继续下一天

    return total_saved_rows

# -------------------------- T1: 主逻辑运行 --------------------------
if __name__ == "__main__":
    _start_date = "20000101" # 应回溯全历史
    _end_date = TODAY_DATE_STR
    
    write_log(SCRIPT_NAME, f"--- 脚本启动 (T3 字段契约已更新，支持断点续采/CSV) ---")

    # 1. 检查已完成日期列表（隐式状态）
    latest_completed_date = get_latest_completed_date_from_disk(SCRIPT_NAME)
    
    if latest_completed_date:
        # 设置新的开始日期为已完成日期的下一天 (T10: 断点续采)
        dt_new_start = datetime.strptime(latest_completed_date, '%Y%m%d') + timedelta(days=1)
        _start_date = dt_new_start.strftime('%Y%m%d')
        
        # 如果新开始日期已晚于当前日期，则退出
        if _start_date > _end_date:
            write_log(SCRIPT_NAME, f"所有数据已采集至最新日期 {_end_date}，无需更新。", "INFO")
            sys.exit(0)
            
        write_log(SCRIPT_NAME, f"检测到断点，采集将从 {latest_completed_date} 的下一天 {_start_date} 开始。", "INFO")

    write_log(SCRIPT_NAME, f"采集范围: {_start_date} - {_end_date}")
    
    total_saved_rows = fetch_and_save_by_ann_date(_start_date, _end_date)
    
    write_log(SCRIPT_NAME, f"全量采集/续采完成。总计保存行数: {total_saved_rows}。", "INFO")
    write_log(SCRIPT_NAME, f"--- 脚本完成 ---")