from odps import ODPS
import pandas as pd
import numpy as np
from datetime import datetime

# 1. 初始化 ODPS 连接
odps = ODPS(
    access_id='your_access_id',  # 替换为你的 Access ID
    secret_access_key='your_secret_key',  # 替换为你的 Secret Key
    project='your_project',  # 替换为你的项目名称
    endpoint='http://service.cn.maxcompute.aliyun.com/api'
)

# 2. 读取数据字典表
def read_data_dict():
    # 假设数据字典表存储在 MaxCompute 中，表名为 data_dictionary
    data_dict_table = odps.get_table('data_dictionary')
    with data_dict_table.open_reader() as reader:
        data_dict = pd.DataFrame(reader.to_pandas())
    return data_dict

# 3. 生成示例数据表
def generate_sample_data(table_name, fields, field_types):
    # 生成示例数据
    sample_data = {}
    for field, field_type in zip(fields, field_types):
        if field_type == 'string':
            sample_data[field] = [f"sample_{i}" if i % 3 != 0 else ("NULL" if i % 2 == 0 else "") for i in range(10)]
        elif field_type == 'bigint':
            sample_data[field] = [i if i % 4 != 0 else -i for i in range(10)]
        elif field_type == 'datetime':
            sample_data[field] = [datetime.now().strftime('%Y-%m-%d %H:%M:%S') if i % 5 != 0 else "invalid_date" for i in range(10)]
        elif field_type == 'amount':
            sample_data[field] = [i * 100 if i % 3 != 0 else -i * 100 for i in range(10)]
    return pd.DataFrame(sample_data)

# 4. 数据质量检查函数
def check_data_quality(df, table_name, fields, pk_fields, field_types):
    issues = {
        "null_values": set(),  # 记录有空值的字段
        "duplicates": set(),   # 记录有重复记录的字段
        "enum_values": set(),  # 记录有枚举值的字段
        "datetime_errors": set(),  # 记录有时间类型错误的字段
        "bigint_errors": set(),    # 记录有数值类型错误的字段
        "amount_errors": set(),    # 记录有金额错误的字段
    }

    # 1. 检查数据量
    data_count = len(df)
    print(f"\n表 {table_name} 的数据量: {data_count}")
    
    # 2. 检查主键唯一性
    if pk_fields:
        pk_columns = [field for field in pk_fields.split(',')]
        duplicate_count = df.duplicated(subset=pk_columns).sum()
        if duplicate_count > 0:
            issues["duplicates"].add(table_name)  # 记录有重复记录的表
        print(f"表 {table_name} 的主键重复记录数: {duplicate_count}")
    
    # 3. 检查空值占比
    for field in fields:
        null_count = df[field].isnull().sum()  # 空值
        empty_count = (df[field] == '').sum()  # 空字符串
        null_str_count = (df[field].astype(str).str.upper() == 'NULL').sum()  # "NULL" 字符串
        total_null = null_count + empty_count + null_str_count
        null_percentage = (total_null / data_count) * 100
        if total_null > 0:
            issues["null_values"].add(field)  # 记录有空值的字段
        print(f"表 {table_name} 的字段 {field} 的空值占比: {null_percentage:.2f}%")
    
    # 4. 检查枚举值
    for field in fields:
        unique_values = df[field].dropna().unique()
        if len(unique_values) < 20:
            issues["enum_values"].add(field)  # 记录有枚举值的字段
            print(f"表 {table_name} 的字段 {field} 的枚举值: {unique_values}")
    
    # 5. 类型检查
    for field, field_type in zip(fields, field_types):
        if field_type == 'datetime':
            try:
                pd.to_datetime(df[field], errors='raise')
            except Exception as e:
                issues["datetime_errors"].add(field)  # 记录有时间类型错误的字段
                print(f"表 {table_name} 的字段 {field} 时间类型转换错误: {e}")
        elif field_type == 'bigint':
            try:
                df[field].astype(np.int64)
            except Exception as e:
                issues["bigint_errors"].add(field)  # 记录有数值类型错误的字段
                print(f"表 {table_name} 的字段 {field} 数值类型转换错误: {e}")
        elif field_type == 'amount':
            try:
                amount_values = df[field].astype(np.int64)
                if (amount_values <= 0).any():
                    issues["amount_errors"].add(field)  # 记录有金额错误的字段
                    print(f"表 {table_name} 的字段 {field} 存在金额小于等于0的值")
            except Exception as e:
                issues["amount_errors"].add(field)  # 记录有金额错误的字段
                print(f"表 {table_name} 的字段 {field} 金额类型转换错误: {e}")
    
    return issues

# 5. 主函数
def main():
    # 读取数据字典
    data_dict = read_data_dict()
    
    # 初始化问题统计
    total_tables = 0
    total_issues = {
        "null_values": set(),  # 记录有空值的字段
        "duplicates": set(),   # 记录有重复记录的表
        "enum_values": set(),  # 记录有枚举值的字段
        "datetime_errors": set(),  # 记录有时间类型错误的字段
        "bigint_errors": set(),    # 记录有数值类型错误的字段
        "amount_errors": set(),    # 记录有金额错误的字段
    }

    # 遍历数据字典，检查每张表
    for table_name, table_data in data_dict.groupby('table_name'):
        fields = table_data['field'].tolist()
        pk_fields = ','.join(table_data[table_data['PK'] == 1]['field'].tolist())
        field_types = table_data['type'].tolist()
        
        # 生成示例数据
        sample_df = generate_sample_data(table_name, fields, field_types)
        print(f"\n正在检查表: {table_name}")
        
        # 检查数据质量
        issues = check_data_quality(sample_df, table_name, fields, pk_fields, field_types)
        
        # 累加问题字段
        for key in total_issues:
            total_issues[key].update(issues[key])
        
        total_tables += 1

    # 输出总结
    print("\n=== 总结 ===")
    print(f"共检查表数量: {total_tables}")
    print(f"有空值的字段数量: {len(total_issues['null_values'])}")
    print(f"有重复记录的表数量: {len(total_issues['duplicates'])}")
    print(f"有枚举值的字段数量: {len(total_issues['enum_values'])}")
    print(f"有时间类型错误的字段数量: {len(total_issues['datetime_errors'])}")
    print(f"有数值类型错误的字段数量: {len(total_issues['bigint_errors'])}")
    print(f"有金额错误的字段数量: {len(total_issues['amount_errors'])}")

    # 找出最常见的问题
    most_common_issue = max(total_issues, key=lambda k: len(total_issues[k]))
    print(f"\n最常见的问题是: {most_common_issue}，涉及 {len(total_issues[most_common_issue])} 个字段或表。")

# 执行主函数
if __name__ == '__main__':
    main()