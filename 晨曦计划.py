import os
import glob
import pandas as pd
from datetime import datetime, timedelta
import re

# ==============================================================================
# --- 配置 ---
# ==============================================================================

STUDENT_INFO_CSV = '学生名单.csv'
NAME_CORRECTIONS = { "D": "邓博", "何沅政DZ250027": "何沅政" }
MORNING_MIN_DURATION = 45
EVENING_MIN_DURATION = 60
HEADER_ROW_NUMBER = 4
PROFESSIONAL_CLASS_NAME = "信息管理与信息系统01"



def load_student_info(csv_path):
    if not os.path.exists(csv_path):
        print(f"找不到学生名单文件 '{csv_path}'。")
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
    # 周四为本周最后一天
    this_thursday = (today - timedelta(days=today.weekday() - 3)).date()
    # 上周五为本周第一天
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

# ==============================================================================
# --- 核心处理函数 (已修改) ---
# --- 接收 year_month 作为参数，不再从文件名中提取 ---
# ==============================================================================
def process_all_data(file_path, student_map, year_month):
    """
    处理单个考勤文件（无论历史或当前），返回有效打卡和异常记录
    (*** 新版本：依赖传入的 year_month 参数 ***)
    """
    print(f"正在处理考勤文件: {file_path} (使用年月: {year_month})")
    try:
        df = pd.read_excel(file_path, sheet_name="打卡时间", header=HEADER_ROW_NUMBER - 1)
    except Exception as e:
        print(f"读取Excel文件 '{file_path}' 时出错。详细错误: {e}")
        return None, None

    # --- 新增逻辑：智能重命名周末列 ---
    # print("开始预处理列名，识别周末...") # 信息过多，在循环中可以注释掉
    new_columns = {}
    last_known_day = 0
    for col in df.columns:
        col_str = str(col)
        if is_number(col_str):
            last_known_day = int(float(col_str))
            new_columns[col] = col_str
        elif "六" in col_str or "日" in col_str:
            if last_known_day > 0:
                inferred_day = last_known_day + 1
                # print(f"识别到周末列 '{col_str}'，推断其日期为: {inferred_day}")
                new_columns[col] = str(inferred_day)
                last_known_day = inferred_day # 更新最后日期，以便正确推算周日
            else:
                # print(f"警告：'{col_str}' 前没有数字日期列，无法推断其日期，将忽略此列。")
                new_columns[col] = col # 保持原样以忽略
        else:
            new_columns[col] = col # 其他列（如“姓名”）保持不变
    
    df.rename(columns=new_columns, inplace=True)
    # --- 新增逻辑结束 ---

    name_column_original = df.columns[0]
    df.rename(columns={name_column_original: "姓名"}, inplace=True)
    df.dropna(subset=['姓名'], inplace=True)
    
    date_columns = [col for col in df.columns if is_number(col)]
    if not date_columns:
        print(f"文件 '{file_path}' 中未找到任何有效的日期列。")
        return None, None
        
    df_long = df.melt(id_vars=["姓名"], value_vars=date_columns, var_name='日', value_name='打卡时间串')
    df_long.dropna(subset=['打卡时间串'], inplace=True)
    df_long['打卡时间列表'] = df_long['打卡时间串'].astype(str).str.findall(r'(\d{2}:\d{2})')
    df_exploded = df_long.explode('打卡时间列表').rename(columns={'打卡时间列表': '打卡时间'})
    df_exploded.dropna(subset=['打卡时间'], inplace=True)
    
    # 使用传入的 year_month 参数
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

    # print(f"文件 {file_path} 得到 {len(df)} 条流水，开始统计...")
    
    valid_sessions, abnormal_records = [], []
    grouped = df.groupby(["姓名", '日期', '时段'])

    for (name, date, period), group in grouped:
        if name not in student_map:
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
            valid_sessions.append({'姓名': name, '日期': date, '时段': period})
        else:
            reason = f"时长不足({int(duration)}分钟)"
            abnormal_records.append({'姓名': name, '日期': date, '打卡时段': period, '问题描述': reason})

    # 注意：有效记录DataFrame只保留'姓名'和'日期'，用于后续去重
    valid_df = pd.DataFrame(valid_sessions)
    if not valid_df.empty:
        valid_df.drop_duplicates(subset=['姓名', '日期', '时段'], inplace=True)

    return valid_df, pd.DataFrame(abnormal_records)

# ==============================================================================
# --- 主函数 (已修改) ---
# --- 增加多文件合并逻辑 ---
# ==============================================================================
def main():
    student_map = load_student_info(STUDENT_INFO_CSV)
    if student_map is None: input("按任意键退出..."); return
    
    start_of_week, end_of_week = get_this_week_range()

    all_valid_sessions_list = []
    all_abnormal_records_list = []

    today = datetime.today()
    current_year = today.year
    current_month = today.month

    # --- 1. 处理所有历史文件 ---
    print(f"\n--- 开始处理历史文件 (基于 {current_year}年{current_month}月 推断年份) ---")
    historical_files = glob.glob("*月份总记录.xlsx")
    
    # 按月份数字排序，确保处理顺序
    sorted_historical_files = sorted(historical_files, key=lambda x: int(re.search(r'(\d+)月份总记录\.xlsx', x).group(1)) if re.search(r'(\d+)月份总记录\.xlsx', x) else 0)
    
    for hist_file in sorted_historical_files:
        hist_match = re.search(r'(\d+)月份总记录\.xlsx', os.path.basename(hist_file))
        if hist_match:
            hist_month = int(hist_match.group(1))
            # 关键逻辑：如果历史文件月份 > 当前月份 (例如 当前1月, 历史12月)，说明历史文件是去年的
            hist_year = current_year - 1 if hist_month > current_month else current_year
            
            year_month_str = f"{hist_year}-{hist_month:02d}-"
            
            valid, abnormal = process_all_data(hist_file, student_map, year_month_str)
            if valid is not None:
                all_valid_sessions_list.append(valid)
                all_abnormal_records_list.append(abnormal)
        else:
            print(f"警告: 历史文件 {hist_file} 命名不规范, 无法提取月份, 已跳过。")

    # --- 2. 处理最新的当前文件 ---
    print("\n--- 开始处理当前文件 ---")
    latest_input_excel = find_latest_file("*_考勤报表_*.xlsx")
    
    if latest_input_excel:
        match = re.search(r'_(\d{8})-', os.path.basename(latest_input_excel))
        if match:
            year_month_str = f"{match.group(1)[:4]}-{match.group(1)[4:6]}-"
            valid, abnormal = process_all_data(latest_input_excel, student_map, year_month_str)
            if valid is not None:
                all_valid_sessions_list.append(valid)
                all_abnormal_records_list.append(abnormal)
        else:
            print(f"警告: 当前文件 {latest_input_excel} 命名不规范, 无法提取年月, 已跳过。")
    else:
        print("未找到当前月份的 *_考勤报表_*.xlsx 文件。")

    # --- 3. 合并与去重 ---
    if not all_valid_sessions_list:
        print("\n未处理任何有效的考勤文件。请检查文件名是否符合规范。")
        input("按任意键退出..."); return

    print("\n--- 开始合并所有数据 ---")
    all_valid_sessions = pd.concat(all_valid_sessions_list)
    all_abnormal_records = pd.concat(all_abnormal_records_list)
    
    print(f"合并前 - 总有效记录: {len(all_valid_sessions)}条, 总异常: {len(all_abnormal_records)}条")

    # 关键：去重，防止历史文件和当前文件有重叠
    all_valid_sessions.drop_duplicates(subset=['姓名', '日期', '时段'], keep='last', inplace=True)
    all_abnormal_records.drop_duplicates(subset=['姓名', '日期', '打卡时段'], keep='last', inplace=True)

    print(f"去重后 - 总有效记录: {len(all_valid_sessions)}条, 总异常: {len(all_abnormal_records)}条")

    # --- 4. 后续统计 (与原逻辑完全一致) ---
    print("\n--- 开始生成统计报告 ---")
    student_df = pd.DataFrame(student_map.items(), columns=['姓名', '学号'])

    total_counts = pd.DataFrame()
    if not all_valid_sessions.empty:
        # 累计打卡此时是 '每人每天算一次'
        total_counts = all_valid_sessions['姓名'].value_counts().reset_index()
        total_counts.columns = ['姓名', '累计打卡次数']
    else:
        print("警告：去重后没有有效的打卡会话。")
        total_counts = student_df[['姓名']].copy()
        total_counts['累计打卡次数'] = 0


    # 筛选本周数据
    this_week_sessions = all_valid_sessions[all_valid_sessions['日期'].between(start_of_week, end_of_week)]
    this_week_counts = pd.DataFrame()
    if not this_week_sessions.empty:
        this_week_counts = this_week_sessions['姓名'].value_counts().reset_index()
        this_week_counts.columns = ['姓名', '本周打卡次数']
    else:
        this_week_counts = student_df[['姓名']].copy()
        this_week_counts['本周打卡次数'] = 0

    this_week_abnormal = pd.DataFrame()
    if not all_abnormal_records.empty:
        this_week_abnormal = all_abnormal_records[all_abnormal_records['日期'].between(start_of_week, end_of_week)]

    # 合并
    final_df = pd.merge(student_df, total_counts, on='姓名', how='left')
    final_df = pd.merge(final_df, this_week_counts, on='姓名', how='left')
    final_df.fillna(0, inplace=True)
    final_df[['累计打卡次数', '本周打卡次数']] = final_df[['累计打卡次数', '本周打卡次数']].astype(int)

    # --- 5. 生成Excel输出 ---
    excel_output_df = final_df[['姓名', '学号', '累计打卡次数']].copy()
    excel_output_df['专业班级'] = PROFESSIONAL_CLASS_NAME
    excel_output_df = excel_output_df[['姓名', '学号', '专业班级', '累计打卡次数']]
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    excel_filename = f'晨曦计划打卡统计结果_{timestamp}.xlsx'
    try:
        excel_output_df.to_excel(excel_filename, sheet_name='累计打卡统计', index=False)
        print(f"\n---> 统计Excel已生成: {excel_filename}")
    except Exception as e:
        print(f"\n*** 错误：无法生成Excel文件！{e} ***")
        print("请确保你没有打开同名文件。")

    
    # --- 6. 生成Markdown周报 ---
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
    try:
        with open(md_filename, 'w', encoding='utf-8') as f:
            f.write(f"# 晨曦计划周报 ({start_of_week} 至 {end_of_week})\n\n")
            f.write(md_df.to_markdown(index=False))
            
        print(f"---> Markdown周报已生成: {md_filename}")
    except Exception as e:
        print(f"\n*** 错误：无法生成Markdown文件！{e} ***")

    input("\n所有任务处理完成，按任意键退出")

if __name__ == "__main__":
    main()