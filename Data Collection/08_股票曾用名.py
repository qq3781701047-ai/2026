import os
import pandas as pd
from datetime import datetime
import sys
import tushare as ts
import time
from tqdm import tqdm

# -------------------------- Tushare 配置 --------------------------
# !!! 替换为您的 Tushare Token !!!
TS_TOKEN = "374f3850d22ecc9f328be05c8f655f274ed5ff0c2c8a45f39fd76092" 
try:
    ts.set_token(TS_TOKEN)
    PRO_API = ts.pro_api()
except Exception:
    print("FATAL ERROR: Tushare Token 配置失败或未配置。请检查 TS_TOKEN 变量。")
    sys.exit(1)
# ------------------------------------------------------------------

# --- 基础配置与通用函数 ---
SCRIPT_NAME = "08_股票曾用名"
BASE_DIR = r"D:\2025"
TODAY_DATE = datetime.now()
TODAY_DATE_STR = TODAY_DATE.strftime('%Y%m%d')
LOG_DIR = os.path.join(BASE_DIR, 'logs', TODAY_DATE_STR)
os.makedirs(LOG_DIR, exist_ok=True) 

# 目标目录结构：直接落地文件，不分区
OUTPUT_BASE_SUBDIR = os.path.join('股票数据', '基础数据', '股票曾用名')
OUTPUT_DIR = os.path.join(BASE_DIR, 'raw', OUTPUT_BASE_SUBDIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 最终需要的字段契约 (namechange 接口返回的字段，根据您的要求)
FIELD_CONTRACT = [
    'ts_code', 'name', 'start_date', 'end_date', 'ann_date', 'change_reason'
]

def write_log(message):
    """写入运行日志文件。"""
    log_file = os.path.join(LOG_DIR, f"{SCRIPT_NAME}_run.log")
    formatted_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}"
    print(formatted_message)
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(formatted_message + '\n')

def run_08_namechange():
    write_log(f"--- {SCRIPT_NAME}.py 开始执行 (V08 - 全量采集) ---")
    
    # 股票曾用名数据最早可追溯到 A 股上市初期
    START_DATE = '19901219'
    END_DATE = TODAY_DATE_STR
    
    # 分段大小：每次请求 5 年的数据，确保不会超限
    segment_days = 365 * 5
    
    current_date = pd.to_datetime(START_DATE, format='%Y%m%d')
    all_data = []
    
    write_log(f"开始分段采集 {START_DATE} 至 {END_DATE} 的股票曾用名历史...")
    
    # namechange 接口频率限制未知，采用保守延时 5 秒/次
    API_CALL_INTERVAL_SECONDS = 5.0 

    # 计算总分段数用于进度条
    total_days = (pd.to_datetime(END_DATE, format='%Y%m%d') - pd.to_datetime(START_DATE, format='%Y%m%d')).days
    num_segments = total_days // segment_days + 1
    
    pbar = tqdm(total=num_segments, desc=f"分段采集 [{SCRIPT_NAME}]")

    while current_date <= pd.to_datetime(END_DATE, format='%Y%m%d'):
        segment_start_date = current_date.strftime('%Y%m%d')
        segment_end_date = (current_date + pd.Timedelta(days=segment_days - 1)).strftime('%Y%m%d')
        
        # 确保分段结束日期不超过今天
        if segment_end_date > END_DATE:
            segment_end_date = END_DATE
        
        # write_log(f"采集时间段: {segment_start_date} - {segment_end_date}") # 注释掉，避免日志过多

        try:
            # namechange 接口：拉取指定时间段内的名称变更事件
            df_change = PRO_API.namechange(start_date=segment_start_date, end_date=segment_end_date)
            
            if not df_change.empty:
                all_data.append(df_change)
                
            # 引入延时机制
            time.sleep(API_CALL_INTERVAL_SECONDS)
            
        except Exception as e:
            if "daily limit" in str(e).lower() or "frequency limit" in str(e).lower() or "最多访问该接口" in str(e):
                write_log(f"API 调用频率限制警告！已停止采集。下次运行请从 {segment_start_date} 处继续。")
            else:
                write_log(f"致命错误：采集 {segment_start_date} - {segment_end_date} 失败: {e}")
            sys.exit(1)

        # 移动到下一段的开始日期
        current_date = pd.to_datetime(segment_end_date, format='%Y%m%d') + pd.Timedelta(days=1)
        pbar.update(1)
        
        # 如果 segment_end_date 已经是 END_DATE，则退出循环
        if segment_end_date == END_DATE:
            break

    pbar.close()

    if not all_data:
        write_log("未采集到任何股票曾用名信息。")
        return

    # 步骤 4: 合并数据并落地
    final_df = pd.concat(all_data, ignore_index=True)
    
    # 去重处理，以防 Tushare 数据重复
    # 曾用名应以 ts_code, start_date, name 为唯一组合键去重
    final_df.drop_duplicates(subset=['ts_code', 'start_date', 'name'], keep='first', inplace=True) 
    
    # 严格遵守字段契约和顺序
    cols_to_save = [col for col in FIELD_CONTRACT if col in final_df.columns]
    
    output_path = os.path.join(OUTPUT_DIR, 'namechange_full.csv')
    try:
        final_df[cols_to_save].to_csv(output_path, index=False, encoding='utf-8')
        write_log(f"数据落地成功！总计采集 {len(final_df)} 条记录到 {output_path}")
    except Exception as e:
        write_log(f"数据落地失败: {e}")

    write_log(f"--- {SCRIPT_NAME} 采集完成。---")

if __name__ == '__main__':
    run_08_namechange()