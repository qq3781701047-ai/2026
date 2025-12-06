import os
import pandas as pd
from datetime import datetime
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
API_SLEEP_SECONDS = 0.15 
LOG_DIR = os.path.join(BASE_DIR, 'logs', TODAY_DATE_STR)
os.makedirs(LOG_DIR, exist_ok=True)

# SCRIPT 16: 财务指标数据 (fina_indicator)
# -------------------------- T0: 元信息 --------------------------
SCRIPT_NAME = "16_财务指标数据"
INTERFACE_NAME = "fina_indicator"
PARTITION_KEY = 'end_date' # T4: 报告期分区
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '财务数据', '财务指标数据')
OUTPUT_DIR = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
FILE_EXTENSION = '.csv' # FIX: 更改输出格式为 CSV

# -------------------------- T3: 字段契约 --------------------------
OUTPUT_FIELDS = [
    'ts_code', 'ann_date', 'end_date', 'eps', 'dt_eps', 'total_revenue_ps', 
    'revenue_ps', 'capital_rese_ps', 'surplus_rese_ps', 'undist_profit_ps', 
    'extra_item', 'profit_dedt', 'gross_margin', 'current_ratio', 'quick_ratio', 
    'cash_ratio', 'invturn_days', 'arturn_days', 'inv_turn', 'ar_turn', 
    'ca_turn', 'fa_turn', 'assets_turn', 'op_income', 'valuechange_income', 
    'interst_income', 'daa', 'ebit', 'ebitda', 'fcff', 'fcfe', 'current_exint', 
    'noncurrent_exint', 'interestdebt', 'netdebt', 'tangible_asset', 
    'working_capital', 'networking_capital', 'invest_capital', 'retained_earnings', 
    'diluted2_eps', 'bps', 'ocfps', 'retainedps', 'cfps', 'ebit_ps', 'fcff_ps', 
    'fcfe_ps', 'netprofit_margin', 'grossprofit_margin', 'cogs_of_sales', 
    'expense_of_sales', 'profit_to_gr', 'saleexp_to_gr', 'adminexp_of_gr', 
    'finaexp_of_gr', 'impai_ttm', 'gc_of_gr', 'op_of_gr', 'ebit_of_gr', 
    'roe', 'roe_waa', 'roe_dt', 'roa', 'npta', 'roic', 'roe_yearly', 
    'roa2_yearly', 'roe_avg', 'opincome_of_ebt', 'investincome_of_ebt', 
    'n_op_profit_of_ebt', 'tax_to_ebt', 'dtprofit_to_profit', 'salescash_to_or', 
    'ocf_to_or', 'ocf_to_opincome', 'capitalized_to_da', 'debt_to_assets', 
    'assets_to_eqt', 'dp_assets_to_eqt', 'ca_to_assets', 'nca_to_assets', 
    'tbassets_to_totalassets', 'int_to_talcap', 'eqt_to_talcapital', 
    'currentdebt_to_debt', 'longdeb_to_debt', 'ocf_to_shortdebt', 
    'debt_to_eqt', 'eqt_to_debt', 'eqt_to_interestdebt', 'tangibleasset_to_debt', 
    'tangasset_to_intdebt', 'tangibleasset_to_netdebt', 'ocf_to_debt', 
    'ocf_to_interestdebt', 'ocf_to_netdebt', 'ebit_to_interest', 
    'longdebt_to_workingcapital', 'ebitda_to_debt', 'turn_days', 'roa_yearly', 
    'roa_dp', 'fixed_assets', 'profit_prefin_exp', 'non_op_profit', 
    'op_to_ebt', 'nop_to_ebt', 'ocf_to_profit', 'cash_to_liqdebt', 
    'cash_to_liqdebt_withinterest', 'op_to_liqdebt', 'op_to_debt', 
    'roic_yearly', 'total_fa_trun', 'profit_to_op', 'q_opincome', 
    'q_investincome', 'q_dtprofit', 'q_eps', 'q_netprofit_margin', 
    'q_gsprofit_margin', 'q_exp_to_sales', 'q_profit_to_gr', 'q_saleexp_to_gr', 
    'q_adminexp_to_gr', 'q_finaexp_to_gr', 'q_impair_to_gr_ttm', 'q_gc_to_gr', 
    'q_op_to_gr', 'q_roe', 'q_dt_roe', 'q_npta', 'q_opincome_to_ebt', 
    'q_investincome_to_ebt', 'q_dtprofit_to_profit', 'q_salescash_to_or', 
    'q_ocf_to_sales', 'q_ocf_to_or', 'basic_eps_yoy', 'dt_eps_yoy', 
    'cfps_yoy', 'op_yoy', 'ebt_yoy', 'netprofit_yoy', 'dt_netprofit_yoy', 
    'ocf_yoy', 'roe_yoy', 'bps_yoy', 'assets_yoy', 'eqt_yoy', 'tr_yoy', 
    'or_yoy', 'q_gr_yoy', 'q_gr_qoq', 'q_sales_yoy', 'q_sales_qoq', 
    'q_op_yoy', 'q_op_qoq', 'q_profit_yoy', 'q_profit_qoq', 'q_netprofit_yoy', 
    'q_netprofit_qoq', 'equity_yoy', 'rd_exp', 'update_flag'
]
PK_COLUMNS = ['ts_code', 'end_date'] # T8: 主键

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

def save_data_to_partition_csv(script_name, df, partition_value):
    """
    专为本脚本重写的保存函数：按 end_date 分区，并以 CSV 格式保存。
    该函数会自动追加（Append）数据，用于将不同股票的数据写入同一报告期文件。
    """
    partition_dir = os.path.join(OUTPUT_DIR, f"{PARTITION_KEY}={partition_value}")
    os.makedirs(partition_dir, exist_ok=True)
    output_path = os.path.join(partition_dir, f"{script_name}{FILE_EXTENSION}")
    
    file_exists = os.path.exists(output_path)
    
    try:
        if file_exists:
            df_existing = pd.read_csv(output_path, encoding='utf-8')
            
            # 检查当前要写入的股票数据是否已经存在于该报告期文件
            current_ts_codes = df['ts_code'].unique().tolist()
            
            for code in current_ts_codes:
                # 检查同一报告期内该股票是否已存在
                is_duplicate = ((df_existing['ts_code'] == code) & (df_existing[PARTITION_KEY] == partition_value)).any()
                if is_duplicate:
                    write_log(script_name, f"报告期 {partition_value} 中股票 {code} 已存在，跳过追加。", "DEBUG")
                    df = df[df['ts_code'] != code] # 从待写入数据中移除
        
        if df.empty:
            return 0
        
        # 写入或追加 CSV
        df.to_csv(
            output_path, 
            index=False, 
            mode='a', 
            header=not file_exists, # 只有第一次写入时带上表头
            encoding='utf-8'
        )
        write_log(script_name, f"分区 {partition_value} 成功追加 {len(df)} 行。", "DEBUG")
        return len(df)
        
    except Exception as e:
        fatal_exit(script_name, f"保存分区 {partition_value} 失败: {e}")

# -------------------------- T5: 对齐与集合 (获取所有股票代码) --------------------------
def get_all_ts_codes(script_name):
    """获取全量股票列表（用于财务数据循环）。"""
    try:
        df_stock = PRO_API.stock_basic(exchange='', list_status='L', fields='ts_code')
        return df_stock['ts_code'].tolist()
    except Exception as e:
        fatal_exit(script_name, f"获取股票列表失败: {e}")

# -------------------------- NEW: 隐式状态检查（实现断点续采） --------------------------
def get_completed_ts_codes_from_disk(script_name, all_codes):
    """
    通过检查磁盘上的 CSV 文件（隐式状态），确定哪些股票已完成采集。
    策略：找到最新的报告期分区，读取其中的 ts_code 列表，视为已完成。
    """
    if not os.path.exists(OUTPUT_DIR):
        return []

    # 1. 找到本地最新的报告期分区
    partition_dirs = [d for d in os.listdir(OUTPUT_DIR) 
                      if os.path.isdir(os.path.join(OUTPUT_DIR, d)) and d.startswith(f"{PARTITION_KEY}=")]
    
    if not partition_dirs:
        return []
        
    latest_partition_value = max([d.split('=')[-1] for d in partition_dirs if len(d.split('=')[-1]) == 8], default=None)
    
    if not latest_partition_value:
        return []
    
    # 2. 检查最新分区的文件完整性
    latest_partition_path = os.path.join(OUTPUT_DIR, f"{PARTITION_KEY}={latest_partition_value}")
    file_path = os.path.join(latest_partition_path, f"{SCRIPT_NAME}{FILE_EXTENSION}")
    
    if not os.path.exists(file_path):
        write_log(script_name, f"最新分区 {latest_partition_value} 中未找到 CSV 文件，重新采集。", "WARNING")
        return []

    # 3. 读取该文件，获取已采集的 ts_code 列表
    try:
        df_latest = pd.read_csv(file_path, encoding='utf-8')
        completed_codes = df_latest['ts_code'].unique().tolist()
        
        # 如果最新分区中的股票数量接近或等于总股票数量的阈值，则视为续采基准
        # 考虑到财务数据发布时间不一致，这里放宽标准，只要有数据就视为已完成该股票
        if len(completed_codes) > 0:
             write_log(script_name, f"基于最新报告期 {latest_partition_value}，找到 {len(completed_codes)} 个已完成股票。", "INFO")
             return completed_codes
        
        write_log(SCRIPT_NAME, f"最新报告期 {latest_partition_value} 包含 0 支股票，可能不完整，将重新采集。", "WARNING")
        return []
        
    except Exception as e:
        write_log(script_name, f"读取最新分区文件失败: {e}，将重新采集。", "ERROR")
        return []

# -------------------------- 核心采集逻辑 --------------------------
def fetch_and_save_by_stock(ts_code, start_date, end_date):
    """获取单只股票全历史财务指标，并按报告期分区保存。"""
    
    df_stock = None
    for attempt in range(MAX_RETRY):
        try:
            time.sleep(API_SLEEP_SECONDS) # T6: API 调用延时
            
            df_stock = PRO_API.fina_indicator(
                ts_code=ts_code, 
                start_date=start_date, 
                end_date=end_date,
                fields=','.join(OUTPUT_FIELDS)
            )
            
            break 
        except Exception as e:
            if attempt < MAX_RETRY - 1:
                write_log(SCRIPT_NAME, f"股票 {ts_code} API 失败，重试 {attempt + 1}/{MAX_RETRY}: {e}", "WARNING")
                time.sleep(API_SLEEP_SECONDS * (2 ** (attempt + 1)))
            else:
                write_log(SCRIPT_NAME, f"股票 {ts_code} 最终失败，跳过: {e}", "ERROR")
                return 0
    
    if df_stock is None or df_stock.empty:
        return 0

    # FIX: 解决重复主键问题 (T10: 幂等性)
    # Tushare 接口可能返回重复的 end_date，原因是财务追溯调整，需保留 ann_date 最晚的一条记录。
    
    # 1. 排序：按报告期和公告日升序排列
    df_stock = df_stock.sort_values(by=['end_date', 'ann_date'], ascending=[True, True])
    
    # 2. 去重：删除重复的 ts_code + end_date 记录，保留最后一条（即公告日最晚的一条）
    initial_rows = len(df_stock)
    df_stock.drop_duplicates(subset=['ts_code', 'end_date'], keep='last', inplace=True)
    if len(df_stock) < initial_rows:
        write_log(SCRIPT_NAME, f"股票 {ts_code} 成功去重 {initial_rows - len(df_stock)} 行（保留最新公告日数据）。", "DEBUG")

    # 3. 最终断言：确保去重后没有重复
    if df_stock.duplicated(subset=['ts_code', 'end_date']).any():
        # 如果去重后仍然出现重复，说明数据质量极差，仍应停止
        fatal_exit(SCRIPT_NAME, f"股票 {ts_code} 数据去重后仍存在重复主键，请检查逻辑。") 
        
    df_stock = df_stock.rename(columns={'end_date': PARTITION_KEY})
    
    # 4. 逐报告期保存数据
    total_rows = 0
    unique_end_dates = df_stock[PARTITION_KEY].unique()
    
    for end_date in unique_end_dates:
        df_partition = df_stock[df_stock[PARTITION_KEY] == end_date]
        rows_saved = save_data_to_partition_csv(SCRIPT_NAME, df_partition, end_date)
        total_rows += rows_saved
        
    return total_rows

# -------------------------- T1: 主逻辑运行 --------------------------
if __name__ == "__main__":
    _start_date = "20000101" # 财务数据应回溯全历史
    _end_date = TODAY_DATE_STR
    
    write_log(SCRIPT_NAME, f"--- 脚本启动 (已修复重复主键问题) ---")
    write_log(SCRIPT_NAME, f"采集范围: {_start_date} - {_end_date}")

    # 1. 获取全量股票代码
    all_codes = get_all_ts_codes(SCRIPT_NAME)
    
    # 2. 检查已完成股票列表（隐式状态）
    completed_codes = get_completed_ts_codes_from_disk(SCRIPT_NAME, all_codes)
    
    # 3. 确定需要采集的股票列表 (断点续采逻辑)
    codes_to_fetch = [code for code in all_codes if code not in completed_codes]
    
    write_log(SCRIPT_NAME, f"需采集股票数量: {len(codes_to_fetch)}/{len(all_codes)}")
    
    total_rows_saved = 0
    
    # 4. 逐只股票采集和保存
    for ts_code in tqdm(codes_to_fetch, desc=f"Fetching {INTERFACE_NAME}"):
        rows = fetch_and_save_by_stock(ts_code, _start_date, _end_date)
        total_rows_saved += rows

    write_log(SCRIPT_NAME, f"全量采集/续采完成。总计保存行数: {total_rows_saved}。")
    write_log(SCRIPT_NAME, f"--- 脚本完成 ---")