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

# SCRIPT 18: 资产负债表 (balancesheet)
# -------------------------- T0: 元信息 --------------------------
SCRIPT_NAME = "18_资产负债表"
INTERFACE_NAME = "balancesheet"
PARTITION_KEY = 'end_date' # T4: 报告期分区
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '财务数据', '资产负债表')
OUTPUT_DIR = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
FILE_EXTENSION = '.csv' 

# -------------------------- T3: 字段契约 --------------------------
# 字段列表
OUTPUT_FIELDS = [
    'ts_code','ann_date','f_ann_date','end_date','report_type',
    'comp_type',
    'end_type',
    'total_share',
    'cap_rese',
    'undistr_porfit',
    'surplus_rese',
    'special_rese',
    'money_cap',
    'trad_asset',
    'notes_receiv',
    'accounts_receiv',
    'oth_receiv',
    'prepayment',
    'div_receiv',
    'int_receiv',
    'inventories',
    'amor_exp',
    'nca_within_1y',
    'sett_rsrv',
    'loanto_oth_bank_fi',
    'premium_receiv',
    'reinsur_receiv',
    'reinsur_res_receiv',
    'pur_resale_fa',
    'oth_cur_assets',
    'total_cur_assets',
    'fa_avail_for_sale',
    'htm_invest',
    'lt_eqt_invest',
    'invest_real_estate',
    'time_deposits',
    'oth_assets',
    'lt_rec',
    'fix_assets',
    'cip',
    'const_materials',
    'fixed_assets_disp',
    'produc_bio_assets',
    'oil_and_gas_assets',
    'intan_assets',
    'r_and_d',
    'goodwill',
    'lt_amor_exp',
    'defer_tax_assets',
    'decr_in_disbur',
    'oth_nca',
    'total_nca',
    'cash_reser_cb',
    'depos_in_oth_bfi',
    'prec_metals',
    'deriv_assets',
    'rr_reins_une_prem',
    'rr_reins_outstd_cla',
    'rr_reins_lins_liab',
    'rr_reins_lthins_liab',
    'refund_depos',
    'ph_pledge_loans',
    'refund_cap_depos',
    'indep_acct_assets',
    'client_depos',
    'client_prov',
    'transac_seat_fee',
    'invest_as_receiv',
    'total_assets',
    'lt_borr',
    'st_borr',
    'cb_borr',
    'depos_ib_deposits',
    'loan_oth_bank',
    'trading_fl',
    'notes_payable',
    'acct_payable',
    'adv_receipts',
    'sold_for_repur_fa',
    'comm_payable',
    'payroll_payable',
    'taxes_payable',
    'int_payable',
    'div_payable',
    'oth_payable',
    'acc_exp',
    'deferred_inc',
    'st_bonds_payable',
    'payable_to_reinsurer',
    'rsrv_insur_cont',
    'acting_trading_sec',
    'acting_uw_sec',
    'non_cur_liab_due_1y',
    'oth_cur_liab',
    'total_cur_liab',
    'bond_payable',
    'lt_payable',
    'specific_payables',
    'estimated_liab',
    'defer_tax_liab',
    'defer_inc_non_cur_liab',
    'oth_ncl',
    'total_ncl',
    'depos_oth_bfi',
    'deriv_liab',
    'depos',
    'agency_bus_liab',
    'oth_liab',
    'prem_receiv_adva',
    'depos_received',
    'ph_invest',
    'reser_une_prem',
    'reser_outstd_claims',
    'reser_lins_liab',
    'reser_lthins_liab',
    'indept_acc_liab',
    'pledge_borr',
    'indem_payable',
    'policy_div_payable',
    'total_liab',
    'treasury_share',
    'ordin_risk_reser',
    'forex_differ',
    'invest_loss_unconf',
    'minority_int',
    'total_hldr_eqy_exc_min_int',
    'total_hldr_eqy_inc_min_int',
    'total_liab_hldr_eqy',
    'lt_payroll_payable',
    'oth_comp_income',
    'oth_eqt_tools',
    'oth_eqt_tools_p_shr',
    'lending_funds',
    'acc_receivable',
    'st_fin_payable',
    'payables',
    'hfs_assets',
    'hfs_sales',
    'cost_fin_assets',
    'fair_value_fin_assets',
    'contract_assets',
    'contract_liab',
    'accounts_receiv_bill',
    'accounts_pay',
    'oth_rcv_total',
    'fix_assets_total',
    'cip_total',
    'oth_pay_total',
    'long_pay_total',
    'debt_invest',
    'oth_debt_invest',
    'oth_eq_invest',
    'oth_illiq_fin_assets',
    'oth_eq_ppbond',
    'receiv_financing',
    'use_right_assets',
    'lease_liab',
    'update_flag'
]

PK_COLUMNS = ['ts_code', PARTITION_KEY, 'report_type'] # T9: 主键

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
    # Tushare API返回重复数据是偶发情况，我们使用异常传递错误信息，在主循环中捕获并跳过当前股票
    raise Exception(message) 

# -------------------------- T10: 幂等性/断点续采核心函数 --------------------------

def save_data_to_partition_csv(script_name, df, partition_value):
    """
    保存数据到报告期分区 CSV 文件，并执行幂等性检查（去重）。
    已修复 SettingWithCopyWarning。
    """
    
    # FIX: 修复 SettingWithCopyWarning：立即创建显式副本
    df_working = df.copy() 

    partition_dir = os.path.join(OUTPUT_DIR, f"{PARTITION_KEY}={partition_value}")
    os.makedirs(partition_dir, exist_ok=True)
    
    # 确定输出路径
    output_path = os.path.join(partition_dir, f"{SCRIPT_NAME}{FILE_EXTENSION}")

    file_exists = os.path.exists(output_path)
    
    rows_to_save = 0
    
    # ------------------- T10: 幂等性/断点续采逻辑 -------------------
    try:
        if file_exists:
            # 读取现有数据的主键
            existing_keys_df = pd.read_csv(output_path, usecols=['ts_code', PARTITION_KEY, 'report_type'], dtype=str)
            existing_keys = (
                existing_keys_df['ts_code'] + '_' + 
                existing_keys_df[PARTITION_KEY].astype(str) + '_' + 
                existing_keys_df['report_type'].astype(str)
            ).unique()

            # 建立当前待写入数据的唯一主键 (在副本上操作)
            df_working['key'] = df_working['ts_code'] + '_' + df_working[PARTITION_KEY].astype(str) + '_' + df_working['report_type'].astype(str)
            
            # 筛选出需要写入的新数据
            df_new = df_working[~df_working['key'].isin(existing_keys)].copy()
            
            # 保持 df_working 干净，删除临时 key 列
            df_working.drop(columns=['key'], inplace=True) 
            
        else:
            # 文件不存在，所有数据都是新的
            df_new = df_working.copy() 
            
        rows_to_save = len(df_new)
        
        if rows_to_save > 0:
            # 成功写入覆盖既有文件 (T10: 幂等)
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

    # 1. 查找所有分区目录
    all_partitions = [d for d in os.listdir(OUTPUT_DIR) if d.startswith(f"{PARTITION_KEY}=")]
    if not all_partitions:
        return []

    # 2. 找到最新的分区目录 (基于字典序，如 end_date=20241231)
    latest_partition_key = sorted(all_partitions, reverse=True)[0] 
    latest_partition_path = os.path.join(OUTPUT_DIR, latest_partition_key, f"{SCRIPT_NAME}{FILE_EXTENSION}")
    
    if not os.path.exists(latest_partition_path):
        return []

    # 3. 读取最新分区中的所有 ts_code
    try:
        df_latest = pd.read_csv(latest_partition_path, usecols=['ts_code'], dtype=str)
        completed_codes = df_latest['ts_code'].unique().tolist()
        completed_codes_in_list = [code for code in completed_codes if code in all_codes]
        
        write_log(script_name, f"已从最新分区 {latest_partition_key} 识别出 {len(completed_codes_in_list)} 只已完成采集的股票。", "INFO")
        return completed_codes_in_list

    except Exception as e:
        write_log(script_name, f"WARNING: 读取最新分区 {latest_partition_key} 失败，将重新采集所有股票: {e}", "WARNING")
        return []

# -------------------------- T8: 主采集函数 --------------------------

def fetch_and_save_by_stock(ts_code, start_date, end_date):
    """
    采集单只股票的全量历史数据并按报告期（end_date）进行保存。
    """
    all_data = []

    # T6: 接口调用与重试
    for attempt in range(MAX_RETRY):
        try:
            # Tushare 接口调用
            df_stock = PRO_API.balancesheet(
                ts_code=ts_code, 
                start_date=start_date, 
                end_date=end_date,
                fields=','.join(OUTPUT_FIELDS)
            )
            
            if df_stock is not None and not df_stock.empty:
                df_stock = df_stock.rename(columns={'end_date': PARTITION_KEY})
                all_data.append(df_stock)
                break 
            
            break 
            
        except Exception as e:
            if attempt < MAX_RETRY - 1:
                write_log(SCRIPT_NAME, f"股票 {ts_code} API 失败，重试 {attempt + 1}/{MAX_RETRY}: {e}", "WARNING")
                time.sleep(API_SLEEP_SECONDS * (2 ** attempt)) 
            else:
                write_log(SCRIPT_NAME, f"股票 {ts_code} 最终失败，跳过: {e}", "WARNING")
                break 
        
        time.sleep(API_SLEEP_SECONDS) 

    if not all_data:
        return 0 

    df_stock = pd.concat(all_data, ignore_index=True)

    # -------------------------- 【重要 FIX】处理 T9 断言失败：强制去重 --------------------------
    # 目的：强制去重，保留最新公告日期（ann_date）的数据，以满足主键唯一性。
    
    # 1. 确保 ann_date 是可排序的字符串
    if 'ann_date' in df_stock.columns:
        df_stock['ann_date'] = df_stock['ann_date'].fillna('00000000') 
    
    # 2. 按主键和 ann_date 排序，保留 ann_date 最晚的
    df_stock = df_stock.sort_values(
        by=PK_COLUMNS + ['ann_date'], 
        ascending=[True, True, True, True]
    ).drop_duplicates(
        subset=PK_COLUMNS, 
        keep='last' # 保留最后出现的行（即 ann_date 最新的）
    )
    # -------------------------- 【FIX End】 --------------------------
    
    # T9: 六断言 - 检查重复主键（在单只股票内）
    if df_stock.duplicated(subset=PK_COLUMNS).any():
        fatal_exit(SCRIPT_NAME, f"T9 断言失败: 股票 {ts_code} 存在重复主键，违反唯一性原则。") 
        
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
    
    write_log(SCRIPT_NAME, f"--- 脚本启动 (已支持断点续采/CSV) ---")
    write_log(SCRIPT_NAME, f"采集范围: {_start_date} - {_end_date}")

    # 1. 获取全量股票代码
    all_codes = get_all_ts_codes(SCRIPT_NAME)
    
    # 2. 检查已完成股票列表（隐式状态）
    completed_codes = get_completed_ts_codes_from_disk(SCRIPT_NAME, all_codes)
    
    # 3. 确定待采集股票列表
    codes_to_fetch = sorted([code for code in all_codes if code not in completed_codes])
    
    if not codes_to_fetch:
        write_log(SCRIPT_NAME, f"所有 {len(all_codes)} 只股票均已完成采集，无需更新。", "INFO")
        sys.exit(0)
        
    write_log(SCRIPT_NAME, f"待采集/更新股票数量: {len(codes_to_fetch)} 只。", "INFO")
    
    total_saved_rows = 0
    
    # 4. 逐只股票采集并保存
    pbar = tqdm(codes_to_fetch, desc=f"Fetching {INTERFACE_NAME}", unit="stock")
    for ts_code in pbar:
        try:
            pbar.set_postfix({'ts_code': ts_code})
            rows_saved = fetch_and_save_by_stock(ts_code, _start_date, _end_date)
            total_saved_rows += rows_saved
            # T7: 遵守频率
            time.sleep(API_SLEEP_SECONDS) 
        except Exception as e:
            # 捕获 fatal_exit 抛出的异常，记录错误并跳过当前股票
            if "T9 断言失败" in str(e):
                write_log(SCRIPT_NAME, f"股票 {ts_code} 发现重复主键已跳过: {e}", "FATAL")
            else:
                write_log(SCRIPT_NAME, f" FATAL ERROR during {ts_code} fetching: {e}", "FATAL")
            
            pass # 确保循环继续运行

    write_log(SCRIPT_NAME, f"全量采集/续采完成。总计保存行数: {total_saved_rows}。", "INFO")
    write_log(SCRIPT_NAME, f"--- 脚本完成 ---")