from datacleaner_framework import DataCleaner

# 示例数据
data = {
    'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'],
    'price': [100, 105, None, 110, 1000],  # 包含缺失值和异常值
    'volume': [1000, 1000, 1000, 1000, 1000]
}
df = pd.DataFrame(data)

# 初始化 DataCleaner
cleaner = DataCleaner(df)

# 数据检验
cleaner.inspect_data()

# 数据清洗
cleaner.clean_data()

# 数据再检验
cleaner.validate_data()

# 导出数据到 CSV
cleaner.export_to_csv('cleaned_data.csv')

# 保存数据到数据库（需替换为实际的数据库连接信息）
# db_url = 'mysql+pymysql://user:password@localhost/db_name'
# cleaner.save_to_database(db_url, 'cleaned_data_table')
