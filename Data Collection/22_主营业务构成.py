import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import tushare as ts
from tqdm import tqdm 
import time
import glob
import logging
from typing import List, Tuple

# -------------------------- T0: 元信息 (Meta Info) --------------------------
SCRIPT_NAME = "22_主营业务构成"
INTERFACE_NAME = "fina_mainbz"
PARTITION_KEY = 'end_date' # T4: 分区键，按报告期截止日期分区
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '财务数据', '主营业务构成') 
OUTPUT_FORMAT = 'csv' 

# 全局工程配置
BASE_DIR = r"D:\2025" 
TODAY_DATE_STR = datetime.now().strftime('%Y%m%d')
MAX_RETRY = 3 
# T7 频率限制：60次/分钟，安全间隔设置为 1.05 秒 (用于每只股票之间休眠)
API_SLEEP_SECONDS = 1.05 
# 根据您的权限，单次最大提取 100 行。
PAGE_SIZE = 100 
# 此变量仅用于日志记录，实际起始日期将动态获取每只股票的 list_date
DEFAULT_START_DATE = "20000101" 

# -------------------------- Tushare 配置与初始化 --------------------------
# 请替换为您的 Tushare Token
TS_TOKEN = "374f3850d22ecc9f328be05c8f655f274ed5ff0c2c8a45f39fd76092" # <--- 请在此处替换为你的 Tushare Token
try:
    ts.set_token(TS_TOKEN)
    # 移除了 ts.set_address()，兼容你的 Tushare 版本
    PRO_API = ts.pro_api(timeout=60) # 增大超时时间，防止网络中断导致的初始化失败
except Exception as e:
    # T11: 打印出具体的错误信息
    print(f"FATAL ERROR: Tushare API 初始化失败。请检查 Token 是否有效或网络连接是否稳定。详细错误: {e}")
    sys.exit(1)

# -------------------------- T3: 字段契约 (Contract) --------------------------
# 【最终修正：严格根据用户提供的字段列表】
OUTPUT_FIELDS = [
    'ts_code', 'end_date', 'bz_item', 'bz_code', 'bz_sales', 'bz_profit', 
    'bz_cost', 'curr_type', 'update_flag'
]
# 主键通常由 股票代码 + 报告期截止日 + 业务项目 共同构成唯一键
PK_COLUMNS = ['ts_code', 'end_date', 'bz_item'] 

# -------------------------- T6/T7/T11/T10: 核心工程函数 --------------------------
def get_output_dir() -> str:
    """T4: 获取脚本的输出路径 D:\\2025\\raw\\...\\脚本名\\"""
    return os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)

def write_log(message: str, level: str = "INFO", exit_script: bool = False):
    """T7: 写入运行日志文件。"""
    log_dir = os.path.join(BASE_DIR, 'logs', TODAY_DATE_STR)
    log_file_path = os.path.join(log_dir, f"{SCRIPT_NAME}.log") 
    os.makedirs(log_dir, exist_ok=True)
    
    log_level = getattr(logging, level.upper(), logging.INFO)
    
    logger = logging.getLogger(SCRIPT_NAME) 
    # 确保只初始化一次 handler
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        # 文件 handler
        fh = logging.FileHandler(log_file_path, encoding='utf-8')
        fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
        logger.addHandler(fh)
        # 控制台 Stream handler
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
        logger.addHandler(sh)

    logger.log(log_level, f"{SCRIPT_NAME}: {message}") 
    
    if exit_script:
        sys.exit(1)

def fatal_exit(message: str):
    """T11: 致命错误，退出脚本并记录日志。"""
    write_log(message, "FATAL", exit_script=True)

def get_latest_completed_ts_code_from_disk() -> str:
    """T10: 从磁盘读取已完成的分区日期，用于判断采集进度。（按 end_date 分区）"""
    output_dir = get_output_dir()
    pattern = os.path.join(output_dir, f"{PARTITION_KEY}=*", f"*.{OUTPUT_FORMAT}")
    
    completed_dates = []
    for path in glob.glob(pattern):
        try:
            # 从路径中提取 end_date=YYYYMMDD 中的 YYYYMMDD
            date_str = path.split(f'{PARTITION_KEY}=')[-1].split(os.sep)[0]
            if len(date_str) == 8 and date_str.isdigit():
                completed_dates.append(date_str)
        except:
            continue
            
    if not completed_dates:
        return None
        
    return max(completed_dates)

def get_all_stocks_with_list_date() -> List[Tuple[str, str]]:
    """T1: 获取全量股票代码及其上市日期，用于迭代采集。"""
    try:
        # 必须获取 list_date 字段
        df_list = PRO_API.stock_basic(exchange='', list_status='L,D,P', fields='ts_code,name,list_date')
        if df_list is None or df_list.empty:
            fatal_exit("无法获取股票列表 (stock_basic)。")
        
        # 确保 list_date 是 YYYYMMDD 格式的字符串
        df_list['list_date'] = df_list['list_date'].astype(str)
        # 返回 (ts_code, list_date) 的列表
        return df_list[['ts_code', 'list_date']].values.tolist()
    except Exception as e:
        # 如果获取股票列表失败，可能是网络或权限问题
        fatal_exit(f"获取股票列表失败: {e}")
        return []

def save_data_to_partition_csv(df: pd.DataFrame, partition_value: str) -> int:
    """T4/T5/T9: 检查数据完整性，并保存到 CSV 分区文件 (T10: 幂等覆盖)。"""
    if df.empty:
        write_log(f"分区 {partition_value} 无数据，跳过保存。", "DEBUG")
        return 0

    # 1. 检查字段完整性
    missing_cols = list(set(OUTPUT_FIELDS) - set(df.columns))
    if missing_cols:
        fatal_exit(f"T3 断言失败: API 返回数据缺少关键字段 {missing_cols}，请检查接口权限。")

    # 2. 选择需要的字段
    df = df[OUTPUT_FIELDS] 
    
    # 3. 构造分区路径
    output_dir = get_output_dir()
    partition_dir = os.path.join(output_dir, f"{PARTITION_KEY}={partition_value}")
    os.makedirs(partition_dir, exist_ok=True)
    output_path = os.path.join(partition_dir, f"{SCRIPT_NAME.split('_', 1)[1]}.{OUTPUT_FORMAT}")
    
    df_to_save = df.copy() # 使用副本进行操作
    
    # 【T5/T9/T10 关键修正】如果文件已存在，则加载旧数据进行合并和去重
    if os.path.exists(output_path):
        try:
            # 必须使用 dtype=str 读取，以保证合并时数据类型一致性
            df_old = pd.read_csv(output_path, encoding='utf-8', dtype=str)
            
            # 合并新旧数据
            df_combined = pd.concat([df_old, df_to_save], ignore_index=True)
            original_rows = len(df_combined)
            
            # T9: 核心去重逻辑：按定义的 PK_COLUMNS 去重，保留最新记录
            df_combined.drop_duplicates(subset=PK_COLUMNS, keep='last', inplace=True) 
            
            if len(df_combined) < original_rows:
                write_log(f"分区 {partition_value} 合并时存在重复主键（{original_rows - len(df_combined)} 条），已保留最新记录。", "WARNING")
            
            df_to_save = df_combined
            
        except Exception as e:
            # 如果加载旧数据失败（例如文件损坏），则使用本次采集的数据覆盖
            write_log(f"加载旧数据 {output_path} 失败，将强制使用本次数据覆盖。错误: {e}", "WARNING")
            
    # 如果文件不存在，或者加载旧数据失败，则直接使用本次数据进行去重
    else:
        original_rows = len(df_to_save)
        df_to_save.drop_duplicates(subset=PK_COLUMNS, keep='last', inplace=True) 
        if len(df_to_save) < original_rows:
            write_log(f"分区 {partition_value} 内部存在重复主键（{original_rows - len(df_to_save)} 条），已保留最新记录。", "WARNING")


    # 4. 最终保存
    try:
        df_to_save.to_csv(output_path, index=False, encoding='utf-8')
        write_log(f"分区 {partition_value} 最终保存 {len(df_to_save)} 行。", "DEBUG")
        return len(df_to_save)
    except Exception as e:
        fatal_exit(f"保存分区 {partition_value} 失败: {e}")
        return 0
    
def fetch_and_save_by_ts_code(ts_code: str, start_date: str) -> int:
    """T1: 核心采集逻辑：按股票代码采集从上市日期开始的历史数据。"""
    
    all_data = []
    
    # 接口采用分页（limit/offset）模式，需要循环获取
    offset = 0
    while True:
        df_page = None
        for attempt in range(MAX_RETRY):
            try:
                # Tushare 接口调用：fina_mainbz，传入 start_date
                df_page = PRO_API.fina_mainbz(
                    ts_code=ts_code,
                    start_date=start_date, # 传入股票上市日期 list_date
                    fields=','.join(OUTPUT_FIELDS), # 确保只请求 OUTPUT_FIELDS 中的字段
                    limit=PAGE_SIZE, 
                    offset=offset
                )
                
                # 成功获取，跳出重试循环
                break 

            except Exception as e:
                # 针对频率限制增加日志提示和休眠时间
                if "每分钟最多访问" in str(e):
                    write_log(f"股票 {ts_code} API 触发频率限制，休眠 65 秒后重试 {attempt + 1}/{MAX_RETRY}: {e}", "WARNING")
                    time.sleep(65) # 发现频率错误后，强制长时间休眠
                elif attempt < MAX_RETRY - 1:
                    write_log(f"股票 {ts_code} API 失败，重试 {attempt + 1}/{MAX_RETRY}: {e}", "WARNING")
                    time.sleep(API_SLEEP_SECONDS * (2 ** attempt)) 
                else:
                    # 最终失败，记录错误但不退出脚本（允许跳过当前股票）
                    write_log(f"ERROR: 股票 {ts_code} API 最终失败: {e}", "ERROR")
                    return 0
        
        # 检查是否获取到数据
        if df_page is None or df_page.empty:
            break
            
        all_data.append(df_page)
        
        # 检查是否还有下一页数据
        if len(df_page) < PAGE_SIZE:
            break
        
        offset += PAGE_SIZE
        
        # 如果进行了分页（即还有下一页），在页面间休眠
        if len(df_page) == PAGE_SIZE:
            # 此处的休眠是页间休眠，主循环外的休眠是股间休眠，互不影响
            time.sleep(API_SLEEP_SECONDS)

    if not all_data:
        write_log(f"股票 {ts_code} API 返回空数据。", "INFO")
        return 0

    df_result = pd.concat(all_data, ignore_index=True)
    
    # 接口返回的 end_date 是字符串，确保作为分区键
    df_result['end_date'] = df_result['end_date'].astype(str)
    
    # 3. 逐报告期保存数据
    unique_end_dates = df_result[PARTITION_KEY].unique()
    rows_saved_stock = 0
    
    for end_date in unique_end_dates:
        df_partition = df_result[df_result[PARTITION_KEY] == end_date]
        rows_saved = save_data_to_partition_csv(df_partition, end_date)
        rows_saved_stock += rows_saved
        
    return rows_saved_stock


# -------------------------- T1: 主逻辑运行 --------------------------
if __name__ == "__main__":
    
    write_log(f"--- 脚本 {SCRIPT_NAME} 启动 (已修正为使用股票上市日期 list_date 作为起始日期，API 超时已增大) ---")

    # 1. 获取全量股票代码及其上市日期
    all_stocks = get_all_stocks_with_list_date()
    
    # 2. 检查已完成日期列表（此处用 latest_completed_date 辅助判断是否是第一次运行）
    latest_completed_date = get_latest_completed_ts_code_from_disk()
    
    write_log(f"开始采集 {len(all_stocks)} 只股票的主营业务构成全历史数据...", "INFO")
    
    total_saved_rows = 0
    
    # T1: 逐股票代码采集并保存
    for ts_code, start_date in tqdm(all_stocks, desc=f"Fetching {INTERFACE_NAME} by ts_code"):
        try:
            # 调用函数，传入股票代码和其上市日期
            rows_saved = fetch_and_save_by_ts_code(ts_code, start_date)
            total_saved_rows += rows_saved
        except Exception as e:
            # fetch_and_save_by_ts_code 内部已处理错误，这里理论上不应触发
            write_log(f"Unexpected ERROR during {ts_code} processing: {e}", "FATAL")
        
        # T7: 遵守频率。在每只股票采集完成后，强制休眠 1.05 秒。
        time.sleep(API_SLEEP_SECONDS)

    write_log(f"--- 脚本 {SCRIPT_NAME} 运行结束。总共保存 {total_saved_rows} 行数据。---", "INFO")