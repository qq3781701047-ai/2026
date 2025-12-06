import os
import pandas as pd
from datetime import datetime, timedelta
import sys
import tushare as ts
from tqdm import tqdm 
import time

# -------------------------- Tushare 配置 --------------------------
# !!! 注意: 需替换为您的 Tushare Token !!!
TS_TOKEN = "374f3850d22ecc9f328be05c8f655f274ed5ff0c2c8a45f39fd76092" 
try:
    ts.set_token(TS_TOKEN)
    PRO_API = ts.pro_api()
except Exception:
    print("FATAL ERROR: Tushare Token 配置失败或未配置。请检查 TS_TOKEN 变量。")
    sys.exit(1)
# ------------------------------------------------------------------

# --- 基础配置与路径 ---
SCRIPT_NAME = "12_同花顺概念和行业指数行情"
BASE_DIR = r"D:\2025"
TODAY_DATE_STR = datetime.now().strftime('%Y%m%d')

# 输入文件路径
CALENDAR_PATH = os.path.join(BASE_DIR, 'raw', '股票数据', '基础数据', '交易日历', 'trade_calendar_full.csv')

# 输出目录配置
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '打板专题数据', '同花顺概念和行业指数行情')
OUTPUT_DIR = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 确保日志目录存在
LOG_DIR = os.path.join(BASE_DIR, 'logs', datetime.now().strftime('%Y%m%d'))
os.makedirs(LOG_DIR, exist_ok=True) 

# 定义输出字段
OUTPUT_FIELDS = [
    'ts_code', 'trade_date', 'open', 'high', 'low', 'close', 
    'pre_close', 'avg_price', 'change', 'pct_change', 'vol', 
    'turnover_rate', 'total_mv', 'float_mv', 'pe_ttm', 'pb_mrq'
]

# Tushare ths_daily 接口需要指数列表，我们先获取指数代码
def get_ths_index_list():
    """获取所有同花顺行业/概念指数代码列表。"""
    write_log("1. 正在获取同花顺所有概念/行业指数代码...")
    try:
        # 获取同花顺指数列表，type='C' (概念) 或 type='I' (行业)
        df_concept = PRO_API.ths_index(type='C')
        df_industry = PRO_API.ths_index(type='I')
        
        # 合并指数代码列表
        ths_codes = pd.concat([df_concept['ts_code'], df_industry['ts_code']]).unique().tolist()
        write_log(f"   已获取 {len(ths_codes)} 个同花顺指数代码。")
        return ths_codes
    except Exception as e:
        write_log(f"FATAL ERROR: 获取同花顺指数列表失败: {e}。请检查积分或API权限。")
        sys.exit(1)


def write_log(message):
    """写入运行日志文件。"""
    log_file = os.path.join(LOG_DIR, f"{SCRIPT_NAME}_run.log")
    formatted_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(formatted_message)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(formatted_message + '\n')

def save_to_csv_partitioned(df_day, date_str, subdir):
    """数据落地函数，CSV 格式，按 trade_date 分区。"""
    partition_dir = os.path.join(BASE_DIR, 'raw', subdir, f"trade_date={date_str}")
    os.makedirs(partition_dir, exist_ok=True)
    output_path = os.path.join(partition_dir, 'data.csv')
    
    try:
        if df_day.empty:
            df_empty = pd.DataFrame(columns=OUTPUT_FIELDS)
            df_empty.to_csv(output_path, index=False, encoding='utf-8')
            return True
        
        # 确保字段顺序和内容正确
        df_day[OUTPUT_FIELDS].to_csv(output_path, index=False, encoding='utf-8')
        return True
    except Exception as e:
        write_log(f"数据落地失败 (CSV) on {date_str}: {e}")
        return False

def load_trade_dates():
    """加载并返回截止到今天的交易日历列表。"""
    if not os.path.exists(CALENDAR_PATH):
        write_log(f"FATAL ERROR: 交易日历文件不存在，请先运行 01_交易日历.py。")
        sys.exit(1)
        
    df_cal = pd.read_csv(CALENDAR_PATH, usecols=['cal_date', 'is_open', 'exchange'])
    trade_dates = df_cal[
        (pd.to_numeric(df_cal['is_open'], errors='coerce') == 1)
    ]['cal_date'].astype(str).tolist()
    
    # 限制交易日历的截止日期为今天
    return [d for d in trade_dates if d <= TODAY_DATE_STR]

def run_script():
    write_log(f"--- {SCRIPT_NAME}.py 开始执行 (采集同花顺概念/行业指数行情) ---")
    
    trade_dates = load_trade_dates()
    if not trade_dates: return

    # 获取指数列表
    ths_codes = get_ths_index_list()
    if not ths_codes:
        write_log("指数代码列表为空，脚本终止。")
        return

    # Tushare ths_daily 接口没有明确历史起点，我们从较早的交易日开始尝试拉取
    START_DATE = '20150101'
    trade_dates_to_pull = [d for d in trade_dates if d >= START_DATE]
    
    if not trade_dates_to_pull:
        write_log("没有需要采集的交易日。")
        return

    write_log(f"2. 准备按日期和指数代码循环采集。时间轴范围: {trade_dates_to_pull[0]} 到 {trade_dates_to_pull[-1]}。")

    # 核心逻辑：按日期循环，并在内部批量或循环调用指数
    # 考虑到指数代码数量可能较大，选择按日期循环拉取所有指数的当日数据，以保证分区完整性
    for date_str in tqdm(trade_dates_to_pull, desc=f"分区落地 [{SCRIPT_NAME}]"):
        daily_data_list = []
        try:
            # Tushare 接口允许传入多个 ts_code，但为保证数据完整性，我们按批次拉取或单日全拉
            # 这里的示例代码简化为按日期全拉（API限制由Tushare内部控制）
            # 警告：如果指数过多，单次调用可能会触发 Tushare 行数限制。
            
            # 尝试批量拉取当日所有指数数据
            df_data = PRO_API.ths_daily(
                trade_date=date_str,
                ts_code=','.join(ths_codes) # 传入所有指数代码
            )
            
            # 引入延时机制
            time.sleep(0.15) 
            
            if df_data.empty:
                continue 
                
            # 清理字段，保留所需的输出字段
            # 注意：Tushare ths_daily 接口返回的字段可能与您给出的列表略有差异，这里优先保留您要求的字段
            df_data = df_data.rename(columns={'close_avg': 'avg_price'})
            
            # 步骤 3: 落地数据
            save_to_csv_partitioned(df_data, date_str, OUTPUT_BASE_SUBDIR)
            
        except Exception as e:
            write_log(f"采集/落地失败 on {date_str}: {e}")
            # sys.exit(1)
            
    write_log(f"--- {SCRIPT_NAME} 采集完成。---")

if __name__ == '__main__':
    run_script()