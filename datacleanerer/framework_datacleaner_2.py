import pandas as pd
import numpy as np
from scipy.stats import zscore
from sqlalchemy import create_engine

class DataCleaner:
    def __init__(self, data):
        """
        初始化 DataCleaner 类。
        :param data: 输入的 DataFrame。
        """
        self.data = data

    def inspect_data(self):
        """
        数据检验：检查缺失值、异常值和重复数据。
        """
        print("===== 数据检验 =====")
        
        # 检查缺失值
        missing_values = self.data.isnull().sum()
        print("缺失值统计：")
        print(missing_values)
        
        # 检查异常值（假设数值型列）
        numeric_columns = self.data.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            self.data[f'{col}_zscore'] = zscore(self.data[col])
            outliers = self.data[(self.data[f'{col}_zscore'].abs() > 3)]
            print(f"列 {col} 的异常值统计：")
            print(outliers[[col]])
        
        # 检查重复数据
        duplicates = self.data[self.data.duplicated()]
        print("重复数据统计：")
        print(duplicates)

    def clean_data(self, missing_value_strategy='drop', outlier_strategy='mean'):
        """
        数据清洗：处理缺失值和异常值。
        :param missing_value_strategy: 缺失值处理策略（'drop', 'ffill', 'bfill', 'mean', 'median', 'mode'）。
        :param outlier_strategy: 异常值处理策略（'mean', 'median', 'remove'）。
        """
        print("\n===== 数据清洗 =====")
        
        # 处理缺失值
        self._handle_missing_values(missing_value_strategy)
        
        # 处理异常值（假设数值型列）
        self._handle_outliers(outlier_strategy)
        
        # 处理重复数据
        self.data.drop_duplicates(inplace=True)
        print("已删除重复数据。")

    def _handle_missing_values(self, strategy):
        """
        处理缺失值。
        :param strategy: 缺失值处理策略（'drop', 'ffill', 'bfill', 'mean', 'median', 'mode'）。
        """
        if strategy == 'drop':
            self.data.dropna(inplace=True)
            print("已删除缺失值。")
        elif strategy == 'ffill':
            self.data.fillna(method='ffill', inplace=True)
            print("已用前向填充处理缺失值。")
        elif strategy == 'bfill':
            self.data.fillna(method='bfill', inplace=True)
            print("已用后向填充处理缺失值。")
        elif strategy == 'mean':
            for col in self.data.select_dtypes(include=[np.number]).columns:
                self.data[col].fillna(self.data[col].mean(), inplace=True)
            print("已用均值填充缺失值。")
        elif strategy == 'median':
            for col in self.data.select_dtypes(include=[np.number]).columns:
                self.data[col].fillna(self.data[col].median(), inplace=True)
            print("已用中位数填充缺失值。")
        elif strategy == 'mode':
            for col in self.data.select_dtypes(include=[np.number]).columns:
                self.data[col].fillna(self.data[col].mode()[0], inplace=True)
            print("已用众数填充缺失值。")
        else:
            raise ValueError(f"不支持的缺失值处理策略: {strategy}")

    def _handle_outliers(self, strategy):
        """
        处理异常值。
        :param strategy: 异常值处理策略（'mean', 'median', 'remove'）。
        """
        numeric_columns = self.data.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            if strategy == 'mean':
                self.data.loc[self.data[f'{col}_zscore'].abs() > 3, col] = self.data[col].mean()
                print(f"已用均值替换列 {col} 的异常值。")
            elif strategy == 'median':
                self.data.loc[self.data[f'{col}_zscore'].abs() > 3, col] = self.data[col].median()
                print(f"已用中位数替换列 {col} 的异常值。")
            elif strategy == 'remove':
                self.data = self.data[self.data[f'{col}_zscore'].abs() <= 3]
                print(f"已删除列 {col} 的异常值。")
            else:
                raise ValueError(f"不支持的异常值处理策略: {strategy}")
        
        # 删除 Z-score 列
        self.data.drop(columns=[f'{col}_zscore' for col in numeric_columns], inplace=True)

    def validate_data(self):
        """
        数据再检验：验证清洗后的数据质量。
        """
        print("\n===== 数据再检验 =====")
        
        # 检查缺失值
        missing_values = self.data.isnull().sum()
        print("清洗后缺失值统计：")
        print(missing_values)
        
        # 检查异常值
        numeric_columns = self.data.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            self.data[f'{col}_zscore'] = zscore(self.data[col])
            outliers = self.data[(self.data[f'{col}_zscore'].abs() > 3)]
            print(f"列 {col} 的异常值统计：")
            print(outliers[[col]])
        
        # 检查重复数据
        duplicates = self.data[self.data.duplicated()]
        print("清洗后重复数据统计：")
        print(duplicates)

    def export_to_csv(self, file_path):
        """
        导出清洗后的数据到 CSV 文件。
        :param file_path: 文件路径。
        """
        self.data.to_csv(file_path, index=False)
        print(f"\n数据已导出到 {file_path}。")

    def save_to_database(self, db_url, table_name):
        """
        将清洗后的数据保存到数据库。
        :param db_url: 数据库连接 URL（如 'mysql+pymysql://user:password@localhost/db_name'）。
        :param table_name: 数据库表名。
        """
        engine = create_engine(db_url)
        self.data.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"\n数据已保存到数据库表 {table_name} 中。")


# 示例用法
if __name__ == "__main__":
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

    # 数据清洗（使用前向填充处理缺失值，用中位数替换异常值）
    cleaner.clean_data(missing_value_strategy='ffill', outlier_strategy='median')

    # 数据再检验
    cleaner.validate_data()

    # 导出数据到 CSV
    cleaner.export_to_csv('cleaned_data.csv')

    # 保存数据到数据库（需替换为实际的数据库连接信息）
    # db_url = 'mysql+pymysql://user:password@localhost/db_name'
    # cleaner.save_to_database(db_url, 'cleaned_data_table')
