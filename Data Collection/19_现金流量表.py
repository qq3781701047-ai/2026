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
    # T7: 增加 API 请求超时时间为 60 秒
    PRO_API = ts.pro_api(timeout=60)
except Exception as e:
    print(f"FATAL ERROR: Tushare Token 配置失败或未配置。详细错误: {e}")
    sys.exit(1)
# --------------------------------------------------------------------------------

BASE_DIR = r"D:\2025" 
TODAY_DATE_STR = datetime.now().strftime('%Y%m%d')
MAX_RETRY = 3 
# Tushare cashflow 接口限制 400 次/分钟，安全间隔为 0.15 秒 (保守)
API_SLEEP_SECONDS = 0.25 
LOG_DIR = os.path.join(BASE_DIR, 'logs', TODAY_DATE_STR)
os.makedirs(LOG_DIR, exist_ok=True)

# SCRIPT 19: 现金流量表 (cashflow)
# -------------------------- T0: 元信息 --------------------------
SCRIPT_NAME = "19_现金流量表"
INTERFACE_NAME = "cashflow"
PARTITION_KEY = 'end_date' # T4: 报告期分区
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '财务数据', '现金流量表')
OUTPUT_DIR = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
FILE_EXTENSION = '.csv' 

# -------------------------- T3: 字段契约 (Contract) --------------------------
# 【最终修正：根据用户提供的 83 个完整字段列表】
OUTPUT_FIELDS = [
    'ts_code',
    'ann_date',
    'f_ann_date',
    'end_date',
    'comp_type',
    'report_type',
    'end_type',
    'net_profit',
    'finan_exp',
    'c_fr_sale_sg',
    'recp_tax_rends',
    'n_depos_incr_fi',
    'n_incr_loans_cb',
    'n_inc_borr_oth_fi',
    'prem_fr_orig_contr',
    'n_incr_insured_dep',
    'n_reinsur_prem',
    'n_incr_disp_tfa',
    'ifc_cash_incr',
    'n_incr_disp_faas',
    'n_incr_loans_oth_bank',
    'n_cap_incr_repur',
    'c_fr_oth_operate_a',
    'c_inf_fr_operate_a',
    'c_paid_goods_s',
    'c_paid_to_for_empl',
    'c_paid_for_taxes',
    'n_incr_clt_loan_adv',
    'n_incr_dep_cbob',
    'c_pay_claims_orig_inco',
    'pay_handling_chrg',
    'pay_comm_insur_plcy',
    'oth_cash_pay_oper_act',
    'st_cash_out_act',
    'n_cashflow_act',
    'oth_recp_ral_inv_act',
    'c_disp_withdrwl_invest',
    'c_recp_return_invest',
    'n_recp_disp_fiolta',
    'n_recp_disp_sobu',
    'stot_inflows_inv_act',
    'c_pay_acq_const_fiolta',
    'c_paid_invest',
    'n_disp_subs_oth_biz',
    'oth_pay_ral_inv_act',
    'n_incr_pledge_loan',
    'stot_out_inv_act',
    'n_cashflow_inv_act',
    'c_recp_borrow',
    'proc_issue_bonds',
    'oth_cash_recp_ral_fnc_act',
    'stot_cash_in_fnc_act',
    'free_cashflow',
    'c_prepay_amt_borr',
    'c_pay_dist_dpcp_int_exp',
    'incl_dvd_profit_paid_sc_ms',
    'oth_cashpay_ral_fnc_act',
    'stot_cashout_fnc_act',
    'n_cash_flows_fnc_act',
    'eff_fx_flu_cash',
    'n_incr_cash_cash_equ',
    'c_cash_equ_beg_period',
    'c_cash_equ_end_period',
    'c_recp_cap_contrib',
    'incl_cash_rec_saims',
    'uncon_invest_loss',
    'prov_depr_assets',
    'depr_fa_coga_dpba',
    'amort_intang_assets',
    'lt_amort_deferred_exp',
    'decr_deferred_exp',
    'incr_acc_exp',
    'loss_disp_fiolta',
    'loss_scr_fa',
    'loss_fv_chg',
    'invest_loss',
    'decr_def_inc_tax_assets',
    'incr_def_inc_tax_liab',
    'decr_inventories',
    'decr_oper_payable',
    'incr_oper_payable',
    'others',
    'im_net_cashflow_oper_act',
    'conv_debt_into_cap',
    'conv_copbonds_due_within_1y',
    'fa_fnc_leases',
    'im_n_incr_cash_equ',
    'net_dism_capital_add',
    'net_cash_rece_sec',
    'credit_impa_loss',
    'use_right_asset_dep',
    'oth_loss_asset',
    'end_bal_cash',
    'beg_bal_cash',
    'end_bal_cash_equ',
    'beg_bal_cash_equ',
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
    
    # 1. 检查字段完整性
    missing_cols = list(set(OUTPUT_FIELDS) - set(df.columns))
    if missing_cols:
        fatal_exit(script_name, f"T3 断言失败: API 返回数据缺少关键字段 {missing_cols}，请检查接口权限。")

    # FIX: 修复 SettingWithCopyWarning：立即创建显式副本，并选择需要的字段
    df_working = df[OUTPUT_FIELDS].copy() 

    partition_dir = os.path.join(OUTPUT_DIR, f"{PARTITION_KEY}={partition_value}")
    os.makedirs(partition_dir, exist_ok=True)
    
    # 确定输出路径
    # 使用 SCRIPT_NAME.split('_', 1)[1] 提取文件名主题
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
                existing_keys_df['report_type'].astype(str)
            ).unique()

            # 建立当前待写入数据的唯一主键 (在副本上操作)
            df_working['key'] = (
                df_working['ts_code'] + '_' + 
                df_working[PARTITION_KEY].astype(str) + '_' + 
                df_working['report_type'].astype(str)
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
        # T7: Tushare stock_basic 接口限制 5000 次/分钟，调用一次即可
        df_stock = PRO_API.stock_basic(exchange='', list_status='L', fields='ts_code,list_date')
        return df_stock
    except Exception as e:
        fatal_exit(script_name, f"获取股票列表失败: {e}")

# -------------------------- NEW: 隐式状态检查（实现断点续采） --------------------------

def get_completed_ts_codes_from_disk(script_name, all_codes_df):
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
    # 使用 SCRIPT_NAME.split('_', 1)[1] 提取文件名主题
    latest_partition_path = os.path.join(OUTPUT_DIR, latest_partition_key, f"{SCRIPT_NAME.split('_', 1)[1]}{FILE_EXTENSION}")
    
    if not os.path.exists(latest_partition_path):
        return []

    # 3. 读取最新分区中的所有 ts_code
    try:
        df_latest = pd.read_csv(latest_partition_path, usecols=['ts_code'], dtype=str)
        completed_codes = df_latest['ts_code'].unique().tolist()
        all_codes_list = all_codes_df['ts_code'].tolist()
        completed_codes_in_list = [code for code in completed_codes if code in all_codes_list]
        
        write_log(script_name, f"已从最新分区 {latest_partition_key} 识别出 {len(completed_codes_in_list)} 只已完成采集的股票。", "INFO")
        return completed_codes_in_list

    except Exception as e:
        write_log(script_name, f"WARNING: 读取最新分区 {latest_partition_key} 失败，将重新采集所有股票: {e}", "WARNING")
        return []

# -------------------------- T8: 主采集函数 --------------------------

def fetch_and_save_by_stock(ts_code, list_date, end_date):
    """
    采集单只股票的全量历史数据并按报告期（end_date）进行保存。
    """
    all_data = []
    
    # T5: 财务数据从上市日期开始采集
    start_date = list_date

    # T6: 接口调用与重试
    for attempt in range(MAX_RETRY):
        try:
            # Tushare 接口调用
            df_stock = PRO_API.cashflow(
                ts_code=ts_code, 
                start_date=start_date, 
                end_date=end_date,
                fields=','.join(OUTPUT_FIELDS)
            )
            
            if df_stock is not None and not df_stock.empty:
                # cashflow 接口返回的 end_date 已经是 PARTITION_KEY
                all_data.append(df_stock)
                break 
            
            # 如果返回空，也退出重试循环
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
    # 过滤掉报告期为空的记录
    df_stock_valid = df_stock.dropna(subset=[PARTITION_KEY])
    
    unique_end_dates = df_stock_valid[PARTITION_KEY].unique()
    
    for end_date in unique_end_dates:
        df_partition = df_stock_valid[df_stock_valid[PARTITION_KEY] == end_date]
        rows_saved = save_data_to_partition_csv(SCRIPT_NAME, df_partition, end_date)
        total_rows += rows_saved
        
    return total_rows

# -------------------------- T1: 主逻辑运行 --------------------------
if __name__ == "__main__":
    _start_date = "20000101" # 财务数据应回溯全历史
    _end_date = TODAY_DATE_STR
    
    write_log(SCRIPT_NAME, f"--- 脚本启动 (T3 字段契约已更新，支持断点续采/CSV) ---")
    write_log(SCRIPT_NAME, f"采集范围: {_start_date} - {_end_date}")

    # 1. 获取全量股票代码
    all_codes_df = get_all_ts_codes(SCRIPT_NAME)
    all_codes_list = all_codes_df['ts_code'].tolist()
    
    # 2. 检查已完成股票列表（隐式状态）
    completed_codes = get_completed_ts_codes_from_disk(SCRIPT_NAME, all_codes_df)
    
    # 3. 确定待采集股票列表
    # 找到未完成的股票，并获取其上市日期 (list_date)
    codes_to_fetch_df = all_codes_df[~all_codes_df['ts_code'].isin(completed_codes)]
    # 按照 ts_code 排序，以便断点续采后顺序稳定
    codes_to_fetch_df = codes_to_fetch_df.sort_values(by='ts_code')
    
    if codes_to_fetch_df.empty:
        write_log(SCRIPT_NAME, f"所有 {len(all_codes_list)} 只股票均已完成采集，无需更新。", "INFO")
        sys.exit(0)
        
    write_log(SCRIPT_NAME, f"待采集/更新股票数量: {len(codes_to_fetch_df)} 只。", "INFO")
    
    total_saved_rows = 0
    
    # 4. 逐只股票采集并保存
    pbar = tqdm(codes_to_fetch_df.itertuples(), total=len(codes_to_fetch_df), desc=f"Fetching {INTERFACE_NAME}", unit="stock")
    for row in pbar:
        ts_code = row.ts_code
        list_date = row.list_date
        
        try:
            pbar.set_postfix({'ts_code': ts_code})
            # T5: 从上市日期 (list_date) 开始采集财务数据
            rows_saved = fetch_and_save_by_stock(ts_code, list_date, _end_date)
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