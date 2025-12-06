import os
import pandas as pd
from datetime import datetime, timedelta
import sys
from tqdm import tqdm
import numpy as np

# --- 基础配置与路径 ---
SCRIPT_NAME = "03_ST股票列表_通过股票曾用名计算"
BASE_DIR = r"D:\2025"
TODAY_DATE_STR = datetime.now().strftime('%Y%m%d')

# 输入文件路径
CALENDAR_PATH = os.path.join(BASE_DIR, 'raw', '股票数据', '基础数据', '交易日历', 'trade_calendar_full.csv')
NAMECHANGE_PATH = os.path.join(BASE_DIR, 'raw', '股票数据', '基础数据', '股票曾用名', 'namechange_full.csv')

# 输出目录配置
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '基础数据', 'ST股票列表（通过股票曾用名计算）')
OUTPUT_DIR = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 确保日志目录存在 (使用今天的日期作为日志目录)
LOG_DIR = os.path.join(BASE_DIR, 'logs', datetime.now().strftime('%Y%m%d'))
os.makedirs(LOG_DIR, exist_ok=True) 

# ST 识别常量
# 填充空值的日期字符串，用于在没有下一条记录时保证当前状态持续到今天
TOMORROW_DATE = datetime.now().date() + timedelta(days=1)
TOMORROW_DATE_TS = pd.Timestamp(TOMORROW_DATE) # 确保 TOMORROW_DATE 也是 Timestamp 类型

def write_log(message):
    """写入运行日志文件。"""
    log_file = os.path.join(LOG_DIR, f"{SCRIPT_NAME}_run.log")
    formatted_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(formatted_message)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(formatted_message + '\n')

def save_to_csv_partitioned(df_day, date_str, subdir):
    """数据落地函数，CSV 格式，按 trade_date 分区。"""
    output_base_dir = os.path.join(BASE_DIR, 'raw', subdir)
    
    # 分区目录结构：D:\2025\raw\...\ST股票列表\trade_date=YYYYMMDD\data.csv
    partition_dir = os.path.join(output_base_dir, f"trade_date={date_str}")
    os.makedirs(partition_dir, exist_ok=True)
    
    output_path = os.path.join(partition_dir, 'data.csv')
    
    try:
        if df_day.empty:
            # 当天没有 ST 股票，创建一个包含字段的空文件
            df_empty = pd.DataFrame(columns=['ts_code', 'trade_date', 'is_st'])
            df_empty.to_csv(output_path, index=False, encoding='utf-8')
            return True
        
        # 字段修正：确保输出字段是 ts_code, trade_date, is_st
        df_day['trade_date'] = date_str
        df_day['is_st'] = 1 # 只要出现在这个列表里，is_st 就是 1
        
        df_day[['ts_code', 'trade_date', 'is_st']].to_csv(output_path, index=False, encoding='utf-8')
        return True
    except Exception as e:
        write_log(f"数据落地失败 (CSV) on {date_str}: {e}")
        return False

def load_data():
    """加载交易日历和曾用名数据。"""
    if not os.path.exists(CALENDAR_PATH):
        write_log(f"FATAL ERROR: 交易日历文件不存在，请先运行 01_交易日历.py。")
        sys.exit(1)
    if not os.path.exists(NAMECHANGE_PATH):
        write_log(f"FATAL ERROR: 曾用名文件不存在，请确保 {NAMECHANGE_PATH} 存在且已运行 08_股票曾用名.py。")
        sys.exit(1)
        
    write_log(f"1. 正在加载交易日历和曾用名数据...")
    
    # 加载交易日历 (只取开市日)
    df_cal = pd.read_csv(CALENDAR_PATH, usecols=['cal_date', 'is_open', 'exchange'])
    trade_dates = df_cal[
        (df_cal['exchange'] == 'SSE') & 
        (pd.to_numeric(df_cal['is_open'], errors='coerce') == 1)
    ]['cal_date'].astype(str).tolist()
    
    # 限制交易日历的截止日期为今天
    trade_dates = [d for d in trade_dates if d <= TODAY_DATE_STR]
    
    # 加载曾用名数据
    df_name = pd.read_csv(NAMECHANGE_PATH, dtype={'ts_code': str})
    
    return trade_dates, df_name

def safe_date_cast(ts):
    """安全地将 Timestamp 转换为 datetime.date"""
    if pd.isna(ts):
        return None
    return ts.date()

def build_st_intervals(df_name):
    """从曾用名数据中构建 ST 状态的时间区间字典 (已校正结束日期)。"""
    write_log("2. 正在构建 ST 状态时间区间...")
    
    # 转换为日期格式，并排除转换失败的行 (NaT)
    df_name['start_date_dt'] = pd.to_datetime(df_name['start_date'], format='%Y%m%d', errors='coerce')
    df_name['end_date_dt'] = pd.to_datetime(df_name['end_date'], format='%Y%m%d', errors='coerce')
    df_name.dropna(subset=['start_date_dt'], inplace=True) # 至少开始日期不能缺失
    
    # 填充 end_date_dt 中的 NaT 为 TOMORROW_DATE_TS (Timestamp 类型)
    # 这样确保了 'end_date_dt' 列中只有 Timestamp 对象，避免 'datetime.date' object has no attribute 'date'
    df_name['end_date_dt'] = df_name['end_date_dt'].fillna(TOMORROW_DATE_TS)
    
    # 0. 确保数据按股票和时间排序
    df_name.sort_values(['ts_code', 'start_date_dt'], inplace=True)
    df_name.reset_index(drop=True, inplace=True)

    st_intervals_dict = {}

    for ts_code, group in df_name.groupby('ts_code'):
        intervals = []
        
        # 将组转换为列表，便于按位置查找下一条记录
        group_list = group.to_dict('records')
        
        for i, row in enumerate(group_list):
            
            # 仅处理 ST 名称的记录
            if pd.isna(row['name']) or not any(kw in row['name'] for kw in ['*ST', 'ST']):
                continue
            
            # ** 修正点 1: 安全转换 **
            st_start = safe_date_cast(row['start_date_dt'])
            st_name = row['name']
            
            # 候选结束日期 1: Tushare 提供的该名称的结束日期
            st_end_candidate_1 = safe_date_cast(row['end_date_dt']) # 此时 end_date_dt 已经是 Timestamp
            
            # 候选结束日期 2: 下一条名称变更记录的 start_date (即状态改变发生时)
            st_end_candidate_2 = TOMORROW_DATE # 默认值为明天 (datetime.date 类型)
            
            if i + 1 < len(group_list):
                # 存在下一条记录，下一条记录的开始日期是强力候选结束日期
                next_row = group_list[i + 1]
                # ** 修正点 2: 安全转换 **
                st_end_candidate_2 = safe_date_cast(next_row['start_date_dt'])

            # ST 状态的真正结束日期：取 Tushare 提供的 end_date 和下一条记录的 start_date 中的较小值。
            st_end = min(st_end_candidate_1, st_end_candidate_2)

            if st_end <= st_start: 
                # 忽略无效区间（例如 start_date >= end_date）
                continue
                
            intervals.append({
                'start_dt': st_start,
                'end_dt': st_end,
                'st_name': st_name
            })
            
        if intervals:
            st_intervals_dict[ts_code] = intervals

    write_log(f"   已成功构建 {len(st_intervals_dict)} 只股票的历史 ST 状态区间 (已校正结束日期)。")
    return st_intervals_dict

def run_st_calculation():
    write_log(f"--- {SCRIPT_NAME}.py 开始执行 (状态计算与分区落地) ---")
    
    # 步骤 1: 加载数据
    trade_dates, df_name = load_data()
    
    # 步骤 2: 构建 ST 状态区间字典
    st_intervals_dict = build_st_intervals(df_name)
    
    if not st_intervals_dict:
        write_log("由于未构建出任何 ST 区间，脚本终止。")
        return

    # 步骤 3: 确定要处理的时间轴
    min_st_date = min(min(item['start_dt'] for item in intervals) for intervals in st_intervals_dict.values()).strftime('%Y%m%d')
    
    # 过滤交易日历 (确保开始日期正确)
    trade_dates_to_process = [d for d in trade_dates if d >= min_st_date]
    
    if not trade_dates_to_process:
        write_log("没有需要处理的交易日。")
        return

    write_log(f"3. 准备生成每日快照。时间轴范围: {trade_dates_to_process[0]} 到 {trade_dates_to_process[-1]}。")
    write_log(f"   待处理交易日数量: {len(trade_dates_to_process)}")

    # 步骤 4: 遍历交易日，生成每日 ST 列表并分区落地
    for date_str in tqdm(trade_dates_to_process, desc=f"分区落地 [{SCRIPT_NAME}]"):
        current_date = pd.to_datetime(date_str, format='%Y%m%d').date()
        daily_st_list = []
        
        for ts_code, intervals in st_intervals_dict.items():
            for interval in intervals:
                # 核心逻辑：判断当前日期是否落在 ST 区间 [start_dt, end_dt) 内
                # 状态有效性：ST开始日期 <= 当前日期 且 当前日期 < ST结束日期 
                if interval['start_dt'] <= current_date and current_date < interval['end_dt']:
                    # 找到一个有效ST状态，则记录该股票
                    daily_st_list.append({'ts_code': ts_code})
                    break 

        df_day = pd.DataFrame(daily_st_list)
        
        # 步骤 5: 落地数据
        save_to_csv_partitioned(df_day, date_str, OUTPUT_BASE_SUBDIR)
            
    write_log(f"--- {SCRIPT_NAME} 采集完成。ST股票每日列表已按分区存储。---")

if __name__ == '__main__':
    run_st_calculation()