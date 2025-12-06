import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import numpy as np
import tushare as ts
from tqdm import tqdm 
import glob

# -------------------------- Tushare 配置 (保持一致) --------------------------
# !!! 注意: 需与 01/02 脚本使用相同的真实 Tushare Token !!!
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

# 目标目录结构
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '基础数据', 'ST股票列表')
OUTPUT_DIR = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 交易日历文件的位置，用于确定时间轴
CALENDAR_PATH = os.path.join(BASE_DIR, 'raw', '股票数据', '基础数据', '交易日历', 'trade_calendar_full.csv')

# 最终需要的字段契约 (包含溯源信息)
# 注意: stock_st 接口只返回 ts_code, is_st, ann_date, start_date, end_date
# 我们需要映射这些字段到我们的最终契约
FIELD_CONTRACT = [
    'ts_code', 'trade_date', 'st_status', 'st_start_date', 'st_end_date', 
    'ann_date', 'change_reason' 
]

def write_log(message, script_name):
    """写入运行日志文件。"""
    log_file = os.path.join(LOG_DIR, f"{script_name}_run.log")
    formatted_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(formatted_message)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(formatted_message + '\n')

def get_last_collected_date(output_dir):
    """断点续采/增量采集功能：获取已落地数据的最新分区日期。"""
    partition_dirs = glob.glob(os.path.join(output_dir, "trade_date=*"))
    
    # 注意：stock_st 接口数据最早到 20160101
    MIN_START_DATE_TS = '20160101'
    
    if not partition_dirs:
        # 如果没有历史数据，从 Tushare 接口的最早日期开始
        return '20151231' 
    
    dates = [os.path.basename(d).split('=')[-1] for d in partition_dirs]
    latest_date = max(dates)
    
    if latest_date < MIN_START_DATE_TS or latest_date > TODAY_DATE_STR:
        return '20151231' 

    return latest_date

def save_to_csv_partitioned(df_day, script_name, subdir):
    """数据落地函数，CSV 格式，按 trade_date 分区。"""
    # 确保 trade_date 字段存在
    if df_day.empty or 'trade_date' not in df_day.columns:
        return False
        
    date_str = df_day['trade_date'].iloc[0]
    output_base_dir = os.path.join(BASE_DIR, 'raw', subdir)
    
    partition_dir = os.path.join(output_base_dir, f"trade_date={date_str}")
    os.makedirs(partition_dir, exist_ok=True)
    
    output_path = os.path.join(partition_dir, 'data.csv')
    
    try:
        # 严格遵守字段契约和顺序
        # 先确保所有契约字段都存在，缺失的用空字符串填充
        for col in FIELD_CONTRACT:
            if col not in df_day.columns:
                df_day[col] = ''
                
        df_day[FIELD_CONTRACT].to_csv(output_path, index=False, encoding='utf-8')
        return True
    except Exception as e:
        write_log(f"数据落地失败 (CSV) on {date_str}: {e}", script_name)
        sys.exit(1)

def fetch_trade_dates():
    """读取 01_交易日历 的产物，获取全市场的交易日列表。"""
    script_name = "03_ST股票列表"
    write_log(f"检查交易日历文件是否存在: {CALENDAR_PATH}", script_name)
    if not os.path.exists(CALENDAR_PATH):
        write_log(f"FATAL ERROR: 交易日历文件不存在，请先运行 01_交易日历.py。", script_name)
        sys.exit(1)
    
    try:
        df_cal = pd.read_csv(CALENDAR_PATH, usecols=['cal_date', 'is_open', 'exchange'])
        
        write_log(f"交易日历文件包含 {len(df_cal)} 条记录", script_name)

        # 使用 pd.to_numeric 稳健转换 is_open 字段
        df_trade_dates = df_cal[
            (df_cal['exchange'] == 'SSE') & 
            (pd.to_numeric(df_cal['is_open'], errors='coerce') == 1)
        ]['cal_date'].astype(str).tolist() 
        
        write_log(f"筛选 (exchange='SSE' & is_open=1) 后得到 {len(df_trade_dates)} 个有效交易日", script_name)
        if len(df_trade_dates) > 0:
            write_log(f"日期范围: {min(df_trade_dates)} 到 {max(df_trade_dates)}", script_name)
        
        return [d for d in df_trade_dates if d >= '19901219']
        
    except Exception as e:
        write_log(f"读取或解析交易日历文件失败: {e}。请检查字段名(is_open)或数据类型。", script_name)
        sys.exit(1)

def fetch_st_status_daily(trade_date, script_name):
    """Tushare API 调用：使用 stock_st 接口获取单日 ST 状态快照。"""
    
    # V10: 使用 stock_st 接口
    try:
        df_daily_st = PRO_API.stock_st(trade_date=trade_date)
        
        if df_daily_st.empty:
            return pd.DataFrame()
            
        # 1. 统一字段名和增加缺失字段
        df_daily_st = df_daily_st.rename(
            columns={
                'start_date': 'st_start_date',
                'end_date': 'st_end_date',
                'is_st': 'st_status' # 接口返回的是 'Y' 或 'N'，需要转换
            }
        )
        
        # 2. 转换 st_status: 接口返回 'Y' (是ST)，但我们需要 'ST' 或 '*ST'
        # 接口不区分 ST 和 *ST，这需要额外处理。
        # 最佳实践是先拉取基础信息，但为了简化代码，我们先标记为 'ST'
        # 注: 如果需要 *ST 区分，需要额外拉取 stock_basic 接口或 namechange 接口辅助判断。
        # 鉴于 stock_st 的简易性，我们暂时只标记为 'ST'，但它解决了重复问题。
        df_daily_st['st_status'] = df_daily_st['st_status'].replace({'Y': 'ST', 'N': 'N/A'})
        
        # 3. 填充缺失字段以匹配 FIELD_CONTRACT
        df_daily_st['change_reason'] = '' # stock_st 接口不提供该字段，填充为空
        
        # 4. 确保日期字段为字符串，并清理小数点
        for col in ['st_start_date', 'st_end_date', 'ann_date']:
            df_daily_st[col] = df_daily_st[col].astype(str).str.replace('.0', '', regex=False)

        # 5. 过滤掉非 ST 记录（如果存在）
        df_daily_st = df_daily_st[df_daily_st['st_status'] != 'N/A'].copy()

        # 6. 确保 trade_date 字段存在
        df_daily_st['trade_date'] = trade_date

        return df_daily_st

    except Exception as e:
        write_log(f"Tushare API (stock_st) 调用失败 on {trade_date}：{e}。请确认您的 API 权限。", script_name)
        # 如果是权限问题或数据缺失，停止脚本
        sys.exit(1) 

def run_03_st_list():
    script_name = "03_ST股票列表"
    write_log(f"--- {script_name}.py 开始执行 (V10: 日循环采集) ---", script_name)
    
    # 步骤 1: 获取交易日历（时间轴）
    all_trade_dates = fetch_trade_dates()
    
    # 步骤 2: 确定断点续采的起始日期
    last_collected_date = get_last_collected_date(OUTPUT_DIR)
    
    start_date_dt = datetime.strptime(last_collected_date, '%Y%m%d') + timedelta(days=1)
    start_date_str = start_date_dt.strftime('%Y%m%d')
    
    # 过滤交易日历，只保留需要采集的日期 (Tushare 接口限制到 20160101)
    MIN_DATE_TS = '20160101'
    
    trade_dates_to_pull = [d for d in all_trade_dates if d >= start_date_str and d >= MIN_DATE_TS and d <= TODAY_DATE_STR]

    write_log(f"Tushare stock_st 接口限制，最早采集日期为: {MIN_DATE_TS}", script_name)
    write_log(f"断点续采/增量采集：从 {start_date_str} 开始计算每日 ST 状态快照。", script_name)
    write_log(f"待采集交易日数量: {len(trade_dates_to_pull)}", script_name) 
    if not trade_dates_to_pull:
        write_log("没有需要采集的新交易日。", script_name)
        return
        
    # --- 3. 循环交易日，按天调用 API 获取 ST 快照并分区落地 ---
    
    write_log(f"开始生成并分区落地 {len(trade_dates_to_pull)} 天的快照 (按天调用 API)...", script_name)

    # 使用进度条封装交易日循环
    for date_str in tqdm(trade_dates_to_pull, desc=f"分区落地 [{script_name}]"):
        
        # 按天调用 API 获取当日 ST 列表
        df_daily_st = fetch_st_status_daily(trade_date=date_str, script_name=script_name)
        
        if df_daily_st.empty:
            continue
            
        # 执行落地
        save_to_csv_partitioned(df_daily_st, script_name, subdir=OUTPUT_BASE_SUBDIR)
            
    write_log(f"--- {script_name} 历史快照生成完成。---", script_name)

if __name__ == '__main__':
    run_03_st_list()