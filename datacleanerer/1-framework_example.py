import pandas as pd
import numpy as np
from scipy.stats import zscore

# 假设 df 是包含股票或期货数据的 DataFrame
# 示例数据
data = {
    'date': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'],
    'price': [100, 105, None, 110, 1000],  # 包含缺失值和异常值
    'volume': [1000, 1000, 1000, 1000, 1000]
}
df = pd.DataFrame(data)

# 检测缺失值
missing_values = df.isnull().sum()
print("缺失值统计：")
print(missing_values)

# 检测异常值
df['zscore'] = zscore(df['price'])
outliers = df[(df['zscore'].abs() > 3)]
print("异常值统计：")
print(outliers)

# 处理缺失值
df['price'].fillna(df['price'].mean(), inplace=True)

# 处理异常值
df.loc[df['zscore'].abs() > 3, 'price'] = df['price'].mean()

# 处理重复数据
df.drop_duplicates(inplace=True)

# 统一日期格式
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')

# 数据验证
print("清洗后的数据：")
print(df)

# 保存清洗后的数据
df.to_csv('cleaned_data.csv', index=False)
