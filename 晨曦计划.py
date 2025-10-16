import os
import glob
import pandas as pd
from datetime import datetime, timedelta
import re

# ==============================================================================
# --- 配置区 ---
# ==============================================================================

STUDENT_INFO_CSV = '学生名单.csv'
NAME_CORRECTIONS = { "D": "邓博", "何沅政DZ250027": "何沅政" }
MORNING_MIN_DURATION = 45
EVENING_MIN_DURATION = 60
HEADER_ROW_NUMBER = 4
PROFESSIONAL_CLASS_NAME = "信息管理与信息系统01"

# ==============================================================================
# --- 脚本核心逻辑 (变量名修正版) ---
# ==============================================================================

def load_student_info(csv_path):
    if not os.path.exists(csv_path):
        print(f"!! 严重错误：找不到学生名单文件 '{csv_path}'。")
        return None
    try:
        df = pd.read_csv(csv_path, dtype={'学号': str})
        student_map = pd.Series(df['学号'].values, index=df['姓名']).to_dict()
        print("学生名单加载成功。")
        return student_map
    except Exception as e:
        print(f"读取学生名单CSV文件 '{csv_path}' 时出错: {e}")
        return None

def find_latest_file(pattern):
    files = glob.glob(pattern)
    return max(files, key=os.path.getmtime) if files else None

def get_this_week_range():
    today = datetime.today()
    this_thursday = (today - timedelta(days=today.weekday() - 3)).date()
    last_friday = (this_thursday - timedelta(days=6))
    print(f"本周统计周期: 从 {last_friday} 到 {this_thursday}")
    return last_friday, this_thursday

def assign_session(dt_object):
    hour = dt_object.hour
    if 6 <= hour < 12: return "早自习"
    if 18 <= hour < 23: return "晚自习"
    return "无效时段"

def is_number(s):
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False

# --- 这里是修正的地方 ---
# 将参数名从 student_id_map 改为 student_map，与主函数保持一致
def process_all_data(file_path, student_map):
    """
    处理完整的考勤总表，返回包含所有日期记录的有效打卡和异常记录
    """
    print(f"正在处理考勤总表: {file_path}")
    try:
        df = pd.read_excel(file_path, sheet_name="打卡时间", header=HEADER_ROW_NUMBER - 1)
    except Exception as e:
        print(f"!! 错误：无法读取Excel文件 '{file_path}'。详细错误: {e}")
        return None, None

    match = re.search(r'_(\d{8})-', os.path.basename(file_path))
    if not match: return None, None
    year_month = f"{match.group(1)[:4]}-{match.group(1)[4:6]}-"

    name_column_original = df.columns[0]
    df.rename(columns={name_column_original: "姓名"}, inplace=True)
    df.dropna(subset=['姓名'], inplace=True)
    
    date_columns = [col for col in df.columns if is_number(col)]
    if not date_columns: return None, None
        
    df_long = df.melt(id_vars=["姓名"], value_vars=date_columns, var_name='日', value_name='打卡时间串')
    df_long.dropna(subset=['打卡时间串'], inplace=True)
    df_long['打卡时间列表'] = df_long['打卡时间串'].astype(str).str.findall(r'(\d{2}:\d{2})')
    df_exploded = df_long.explode('打卡时间列表').rename(columns={'打卡时间列表': '打卡时间'})
    df_exploded.dropna(subset=['打卡时间'], inplace=True)
    df_exploded['日'] = df_exploded['日'].astype(float).astype(int).astype(str)
    df_exploded['完整时间'] = year_month + df_exploded['日'] + " " + df_exploded['打卡时间']
    
    df = df_exploded
    if NAME_CORRECTIONS:
        df["姓名"] = df["姓名"].replace(NAME_CORRECTIONS)

    df["格式化时间"] = pd.to_datetime(df["完整时间"], errors='coerce', format='%Y-%m-%d %H:%M')
    df.dropna(subset=["格式化时间"], inplace=True)

    df['日期'] = df["格式化时间"].dt.date
    df['时段'] = df["格式化时间"].apply(assign_session)
    df = df[df['时段'] != '无效时段']

    if df.empty:
        return pd.DataFrame(columns=['姓名', '日期']), pd.DataFrame(columns=['姓名', '日期', '打卡时段', '问题描述'])

    print(f"数据重塑成功，共得到 {len(df)} 条独立的打卡流水记录，开始统计...")
    
    valid_sessions, abnormal_records = [], []
    grouped = df.groupby(["姓名", '日期', '时段'])

    for (name, date, period), group in grouped:
        if name not in student_map: # 使用修正后的变量名 student_map
            abnormal_records.append({'姓名': name, '日期': date, '打卡时段': period, '问题描述': "未在学生名单中"})
            continue
        if len(group) < 2:
            abnormal_records.append({'姓名': name, '日期': date, '打卡时段': period, '问题描述': f"次数不足(仅{len(group)}次)"})
            continue
        
        start_time, end_time = group["格式化时间"].min(), group["格式化时间"].max()
        duration = (end_time - start_time).total_seconds() / 60
        
        is_valid, required_duration = False, 0
        if period == "早自习":
            required_duration = MORNING_MIN_DURATION
            if duration >= required_duration: is_valid = True
        elif period == "晚自习":
            required_duration = EVENING_MIN_DURATION
            if duration >= required_duration: is_valid = True
            
        if is_valid:
            valid_sessions.append({'姓名': name, '日期': date})
        else:
            reason = f"时长不足({int(duration)}分钟)"
            abnormal_records.append({'姓名': name, '日期': date, '打卡时段': period, '问题描述': reason})

    return pd.DataFrame(valid_sessions), pd.DataFrame(abnormal_records)

def main():
    student_map = load_student_info(STUDENT_INFO_CSV)
    if student_map is None: input("按任意键退出..."); return
    
    start_of_week, end_of_week = get_this_week_range()

    latest_input_excel = find_latest_file("*_考勤报表_*.xlsx")
    if not latest_input_excel:
        print("!! 错误：在文件夹中未找到任何考勤报表Excel文件。")
        input("按任意键退出..."); return

    all_valid_sessions, all_abnormal_records = process_all_data(latest_input_excel, student_map) # 调用时变量名正确
    if all_valid_sessions is None: 
        print("!! 处理数据时发生致命错误，程序已终止。")
        input("按任意键退出..."); return

    student_df = pd.DataFrame(student_map.items(), columns=['姓名', '学号'])

    total_counts = pd.DataFrame()
    if not all_valid_sessions.empty:
        total_counts = all_valid_sessions['姓名'].value_counts().reset_index()
        total_counts.columns = ['姓名', '累计打卡次数']

    this_week_sessions = all_valid_sessions[all_valid_sessions['日期'].between(start_of_week, end_of_week)]
    this_week_counts = pd.DataFrame()
    if not this_week_sessions.empty:
        this_week_counts = this_week_sessions['姓名'].value_counts().reset_index()
        this_week_counts.columns = ['姓名', '本周打卡次数']

    this_week_abnormal = pd.DataFrame()
    if not all_abnormal_records.empty:
        this_week_abnormal = all_abnormal_records[all_abnormal_records['日期'].between(start_of_week, end_of_week)]

    final_df = pd.merge(student_df, total_counts, on='姓名', how='left')
    final_df = pd.merge(final_df, this_week_counts, on='姓名', how='left')
    final_df.fillna(0, inplace=True)
    final_df[['累计打卡次数', '本周打卡次数']] = final_df[['累计打卡次数', '本周打卡次数']].astype(int)

    excel_output_df = final_df[['姓名', '学号', '累计打卡次数']].copy()
    excel_output_df['专业班级'] = PROFESSIONAL_CLASS_NAME
    excel_output_df = excel_output_df[['姓名', '学号', '专业班级', '累计打卡次数']]
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_filename = f'晨曦计划打卡统计结果_{timestamp}.xlsx'
    excel_output_df.to_excel(excel_filename, sheet_name='累计打卡统计', index=False)
    print(f"\n---> 新的累计统计Excel已生成: {excel_filename}")
    
    md_df = final_df[['姓名', '学号', '本周打卡次数', '累计打卡次数']].copy()
    
    if not this_week_abnormal.empty:
        this_week_abnormal['异常记录文本'] = this_week_abnormal['日期'].astype(str) + " " + this_week_abnormal['打卡时段'] + ": " + this_week_abnormal['问题描述']
        abnormal_summary = this_week_abnormal.groupby('姓名')['异常记录文本'].apply(lambda x: '；'.join(x)).reset_index()
        abnormal_summary.rename(columns={'异常记录文本': '本周异常记录'}, inplace=True)
        md_df = pd.merge(md_df, abnormal_summary, on='姓名', how='left')
    else:
        md_df['本周异常记录'] = ''
        
    md_df.fillna('', inplace=True)
    md_df = md_df.sort_values(by="学号")

    md_filename = f'晨曦计划周报_{timestamp}.md'
    with open(md_filename, 'w', encoding='utf-8') as f:
        f.write(f"# 晨曦计划周报 ({start_of_week} 至 {end_of_week})\n\n")
        f.write(md_df.to_markdown(index=False))
        
    print(f"---> 新的Markdown周报已生成: {md_filename}")

    input("\n所有任务处理完成，按任意键退出...")

if __name__ == "__main__":
    main()