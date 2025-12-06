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
SCRIPT_NAME = "23_股权质押统计数据"
INTERFACE_NAME = "pledge_stat"
PARTITION_KEY = 'end_date' # T4: 分区键，按报告期截止日期分区
# 修正为用户要求的输出目录：D:\2025\raw\股票数据\参考数据\股权质押统计数据\
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '参考数据', '股权质押统计数据') 
OUTPUT_FORMAT = 'csv' # 统一保存为 CSV

BASE_DIR = r"D:\2025" 
TODAY_DATE_STR = datetime.now().strftime('%Y%m%d')
MAX_RETRY = 3 
# 解决 ConnectionResetError，增加休眠时间
API_SLEEP_SECONDS = 0.5 
# Tushare pro接口默认分页大小，必须显式设置 limit/offset 来确保全量采集
# 🌟【修复 NameError】 PAGE_SIZE 变量缺失，已补回
PAGE_SIZE = 5000 
# 报告期数据采集滞后性较大，保守地设置为回溯四个季度（一整年）
REPORT_DELAY_QUARTERS = 4 

# -------------------------- Tushare 配置与初始化 --------------------------
# 请替换为您的 Tushare Token
TS_TOKEN = "374f3850d22ecc9f328be05c8f655f274ed5ff0c2c8a45f39fd76092" 
try:
    ts.set_token(TS_TOKEN)
    PRO_API = ts.pro_api()
except Exception:
    print("FATAL ERROR: Tushare Token 配置失败或未配置。")
    sys.exit(1)

# -------------------------- T3: 字段契约 (Contract) --------------------------
# 字段契约必须包含所有期望字段，用于数据校验
OUTPUT_FIELDS = [
    'ts_code',
    'end_date',
    'pledge_count',
    'unrest_pledge',
    'rest_pledge',
    'total_share',
    'pledge_ratio',
    'update_flag'
]
# 主键 T9：报告期 + 股票代码
PK_COLUMNS = ['ts_code', 'end_date'] 
# 🌟【新增】用于检查完整性的核心字段，缺失超过一半（例如 5 个）就判定为不完整
KEY_FIELD_COUNT_THRESHOLD = 5

# -------------------------- T6/T7/T11/T10: 核心工程函数 --------------------------
def get_output_dir(script_name: str) -> str:
    """T4: 获取脚本的输出路径 D:\\2025\\raw\\...\\脚本名\\"""
    return os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)

def write_log(message: str, level: str = "INFO", exit_script: bool = False):
    """T7: 写入运行日志文件。"""
    log_file_path = os.path.join(BASE_DIR, 'logs', TODAY_DATE_STR, f"{SCRIPT_NAME}.log")
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    # 初始化/配置 Logger
    logger = logging.getLogger(SCRIPT_NAME)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        # File Handler (T7)
        fh = logging.FileHandler(log_file_path, encoding='utf-8')
        fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(fh)
        # Stream Handler (T7)
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
        logger.addHandler(sh)

    # 打印日志
    logger.log(log_level, f"{SCRIPT_NAME}: {message}")
    
    if exit_script:
        sys.exit(1)

def fatal_exit(message: str):
    """T11: 致命错误，退出脚本并记录日志。"""
    write_log(message, "FATAL", exit_script=True)

def get_latest_completed_date_from_disk(script_name: str) -> str:
    """T10: 从磁盘读取已完成的分区日期，实现断点续采。"""
    output_dir = get_output_dir(script_name)
    pattern = os.path.join(output_dir, f"{PARTITION_KEY}=*", f"*.{OUTPUT_FORMAT}")
    
    # 查找所有已保存的分区目录，并提取日期
    completed_dates = []
    for path in glob.glob(pattern):
        # 从路径中提取 'end_date=YYYYMMDD'
        try:
            date_str = path.split(f'{PARTITION_KEY}=')[-1].split(os.sep)[0]
            if len(date_str) == 8 and date_str.isdigit():
                completed_dates.append(date_str)
        except:
            continue
            
    if not completed_dates:
        return None
        
    return max(completed_dates)

def get_report_end_dates(start_date: str, end_date: str) -> List[str]:
    """根据 Tushare 财报逻辑，获取从 start_date 到 end_date 之间的所有季报截止日。"""
    dates = []
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])
    
    for year in range(start_year, end_year + 1):
        # 4个标准的报告期截止日
        dates.append(f"{year}0331")
        dates.append(f"{year}0630")
        dates.append(f"{year}0930")
        dates.append(f"{year}1231")
        
    # 过滤掉不在指定范围内的日期
    dates = [d for d in dates if start_date <= d <= end_date]
    return sorted(list(set(dates)))

# 🌟【修改 1】新增 is_latest_safe_date 参数
def save_data_to_partition_csv(script_name: str, df: pd.DataFrame, partition_value: str, is_latest_safe_date: bool = False) -> int:
    """T4/T5/T9: 检查数据完整性，并保存到 CSV 分区文件 (T10: 幂等覆盖)。"""
    if df.empty:
        write_log(f"日期 {partition_value} 无数据，跳过保存。", "DEBUG")
        return 0

    # T9: 六断言 - 检查重复主键
    if df.duplicated(subset=PK_COLUMNS).any():
        fatal_exit(f"T9 断言失败: 日期 {partition_value} 存在重复主键，违反唯一性原则。")

    # T3: 字段契约检查
    missing_cols = list(set(OUTPUT_FIELDS) - set(df.columns))
    extra_cols = list(set(df.columns) - set(OUTPUT_FIELDS))

    if missing_cols:
        # 🌟【新增检查】如果当前采集的是最新的安全截止日，且存在大量字段缺失，则放弃保存
        if is_latest_safe_date and len(missing_cols) >= KEY_FIELD_COUNT_THRESHOLD:
            write_log(f"致命警告：日期 {partition_value} 为最新安全截止日，但缺少 {len(missing_cols)} 个关键字段。放弃保存，建议等待下次运行或增加 REPORT_DELAY_QUARTERS (>4)。", "CRITICAL")
            return 0 # 返回 0，放弃保存不完整的数据
        
        # 针对 API 数据不完整的场景，打印警告，并用 NaN 填充以保持结构
        for col in missing_cols:
            df[col] = pd.NA
        write_log(f"警告：API 返回数据缺少字段 {missing_cols}，已用 NaN 填充以保持结构。", "WARNING")

    if extra_cols:
        write_log(f"警告：API 返回数据包含额外字段 {extra_cols}，已丢弃。", "WARNING")
        # 重新选择 OUTPUT_FIELDS 确保顺序和内容正确，且包含所有填充的 NA 字段
        df = df[OUTPUT_FIELDS] 

    # T4/T5: 构建分区路径
    partition_dir = os.path.join(get_output_dir(script_name), f"{PARTITION_KEY}={partition_value}")
    os.makedirs(partition_dir, exist_ok=True)
    output_path = os.path.join(partition_dir, f"{SCRIPT_NAME}.{OUTPUT_FORMAT}")
    
    # 成功写入覆盖既有文件 (T10: 幂等)
    try:
        df.to_csv(output_path, index=False, encoding='utf-8')
        write_log(f"日期 {partition_value} 成功保存 {len(df)} 行。", "INFO")
        return len(df)
    except Exception as e:
        # 失败不得残留半文件 (T10) - 写入失败应记录并停止
        fatal_exit(f"保存分区 {partition_value} 失败: {e}")
        return 0

# fetch_and_save_by_date 签名也需要修改
def fetch_and_save_by_date(date_str: str, is_latest_safe_date: bool = False) -> int:
    """T1: 核心采集逻辑：按报告期采集，并处理分页。"""
    offset = 0
    all_data = []
    total_expected_rows = None # 用于分页校验

    write_log(f"开始采集报告期 {date_str}...", "INFO")

    # T1: 分页循环采集，确保获取全量数据
    while True:
        df_day = None
        current_page_rows = 0 # 记录当前页实际行数
        
        for attempt in range(MAX_RETRY):
            try:
                # Tushare 接口调用：按 end_date 分页循环
                df_day = PRO_API.pledge_stat(
                    end_date=date_str, 
                    fields=','.join(OUTPUT_FIELDS),
                    limit=PAGE_SIZE,
                    offset=offset
                )
                
                if df_day is not None and not df_day.empty:
                    # 成功获取数据
                    df_day = df_day.rename(columns={'end_date': PARTITION_KEY})
                    all_data.append(df_day)
                    current_page_rows = len(df_day)
                    break # 退出重试循环
                else:
                    # 返回空数据（可能是最后一页，或无数据）
                    current_page_rows = 0
                    break 
            
            except Exception as e:
                # T6: 指数退避与重试
                if attempt < MAX_RETRY - 1:
                    write_log(f"API 失败，重试 {attempt + 1}/{MAX_RETRY}: {e}", "WARNING")
                    # T7: 使用 API_SLEEP_SECONDS 进行指数退避
                    time.sleep(API_SLEEP_SECONDS * (2 ** attempt)) 
                else:
                    # T11: 最终失败，记录致命错误并退出
                    fatal_exit(f"日期 {date_str} API 最终失败: {e}")
            
            time.sleep(API_SLEEP_SECONDS) # T7: 遵守频率
        
        # 检查是否还有下一页
        if current_page_rows < PAGE_SIZE:
            break # 已到最后一页或无数据
        
        offset += PAGE_SIZE

    if not all_data:
        write_log(f"日期 {date_str} API 返回空数据。", "INFO")
        return 0

    df_result = pd.concat(all_data, ignore_index=True)
    write_log(f"日期 {date_str} 分页采集完成，总共采集 {len(df_result)} 行。", "INFO")
    
    # 修复重复主键问题: 确保主键唯一性，对重复的 ts_code + end_date 数据进行去重
    original_rows = len(df_result)
    # 使用 drop_duplicates 去重，保留第一次出现的数据
    df_result.drop_duplicates(subset=PK_COLUMNS, keep='first', inplace=True) 
    if len(df_result) < original_rows:
        write_log(f"日期 {date_str} 发现 {original_rows - len(df_result)} 条重复主键数据，已去重。", "WARNING")
    
    # 🌟【修改 2】T1: 保存数据到分区，传入 is_latest_safe_date
    return save_data_to_partition_csv(SCRIPT_NAME, df_result, date_str, is_latest_safe_date)

# -------------------------- T1: 主逻辑运行 --------------------------
def get_most_recent_report_date(current_date: datetime) -> str:
    """计算理论上最近的报告期截止日。"""
    if current_date.month >= 10:
        return current_date.strftime('%Y') + '0930'
    elif current_date.month >= 7:
        return current_date.strftime('%Y') + '0630'
    elif current_date.month >= 4:
        return current_date.strftime('%Y') + '0331'
    else:
        return str(current_date.year - 1) + '1231'

def calculate_adjusted_end_date(end_date_str: str, delay_quarters: int) -> str:
    """根据延迟季度数，将报告期截止日期向前推。"""
    year = int(end_date_str[:4])
    month_day = end_date_str[4:]
    
    # 报告期月份映射
    date_map = {
        '0331': 3,
        '0630': 6,
        '0930': 9,
        '1231': 12
    }
    reverse_map = {v: k for k, v in date_map.items()}
    
    # 假设传入的 end_date_str 已经是季度末日期
    if month_day not in date_map:
        raise ValueError("传入的日期不是标准的季度末日期。")
        
    current_month = date_map[month_day]
    
    # 计算需要回退的季度数，每个季度对应 3 个月
    total_months_to_subtract = delay_quarters * 3
    
    # 从当前月份开始回退
    new_month = current_month - total_months_to_subtract
    new_year = year
    
    while new_month <= 0:
        new_year -= 1
        new_month += 12 # 每年 12 个月
    
    # 找到新的月份对应的报告期截止日
    if new_month > 9:
        adjusted_month_day = '1231'
    elif new_month > 6:
        adjusted_month_day = '0930'
    elif new_month > 3:
        adjusted_month_day = '0630'
    else:
        adjusted_month_day = '0331'
        
    return str(new_year) + adjusted_month_day


if __name__ == "__main__":
    _start_date = "20000101" # 股权质押数据应回溯全历史
    current_date = datetime.now()

    # 1. 计算理论上的最新报告期截止日
    theoretical_end_date = get_most_recent_report_date(current_date)
    
    # 2. 根据数据滞后性，将截止日期往前回溯 REPORT_DELAY_QUARTERS 个季度
    _end_date = calculate_adjusted_end_date(theoretical_end_date, REPORT_DELAY_QUARTERS)

    write_log(f"--- 脚本 {SCRIPT_NAME} 启动 (已修正最新数据完整性检查) ---")
    write_log(f"理论最新报告期: {theoretical_end_date}，安全采集截止日 (_end_date): {_end_date}", "INFO")

    # 3. 检查已完成报告期列表（隐式状态）
    latest_completed_date = get_latest_completed_date_from_disk(SCRIPT_NAME)
    
    # 4. 获取目标报告期列表
    all_report_dates = get_report_end_dates(_start_date, _end_date)
    
    if latest_completed_date:
        # 找到第一个大于已完成日期的报告期作为新起始
        dates_to_fetch = [d for d in all_report_dates if d > latest_completed_date]
        if not dates_to_fetch:
            write_log(f"所有报告期 ({_end_date}) 均已采集完成，无需更新。", "INFO")
            sys.exit(0)
    else:
        dates_to_fetch = all_report_dates
        
    write_log(f"采集范围: {dates_to_fetch[0]} - {dates_to_fetch[-1]}，共 {len(dates_to_fetch)} 个报告期。", "INFO")

    total_saved_rows = 0
    
    # T1: 逐报告期采集并保存
    for date_str in tqdm(dates_to_fetch, desc=f"Fetching {INTERFACE_NAME}"):
        
        # 🌟【修改 3】判断是否是当前批次要采集的最后一个日期，用于在保存时进行严格检查
        is_latest_safe = (date_str == _end_date)
             
        try:
            # 🌟 传入 is_latest_safe 参数
            rows_saved = fetch_and_save_by_date(date_str, is_latest_safe)
            total_saved_rows += rows_saved
        except Exception as e:
            # fetch_and_save_by_date 中最终失败会调用 fatal_exit
            write_log(f"FATAL ERROR during {date_str} fetching: {e}", "FATAL")

    write_log(f"--- 脚本 {SCRIPT_NAME} 运行结束。总共保存 {total_saved_rows} 行数据。---", "INFO")