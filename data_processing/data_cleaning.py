import pandas as pd
from sqlalchemy import create_engine, text

# 读取爬取到的图书数据
df = pd.read_json('../data/data.json', lines=True)

# 数据清洗

# 1. 处理价格列，去除'元'字符并转换为浮点数
df['price'] = pd.to_numeric(df['price'].str.replace('元', ''), errors='coerce')

# 2. 将评分列转换为浮点型数据
df['score'] = pd.to_numeric(df['score'], errors='coerce')

# 3. 处理出版日期列
# 第一步：解析完整的 YYYY-MM-DD 格式日期
df['pub_date'] = pd.to_datetime(df['pub_date'], errors='coerce')

# 第二步：对于解析失败的行（NaT 值），提取 YYYY-MM 格式并将其补全为 YYYY-MM-01
mask = df['pub_date'].isna()  # 查找解析失败的行
date_str = df.loc[mask, 'pub_date'].astype(str).str.extract(r'(\d{4}-\d{1,2})')[0]  # 提取年份和月份
df.loc[mask, 'pub_date'] = pd.to_datetime(date_str + '-01', errors='coerce')  # 补全为该月的第一天

# 4. 删除在重要列中存在缺失值的行
df_cleaned = df.dropna(subset=['title', 'author', 'price', 'score', 'pub_date'])

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', 100)

print("清洗后的数据示例：")
print(df_cleaned.head(10))

# 连接 Hive
# engine = create_engine('hive://root@192.168.128.130:10000/default')

# 创建 Hive 表（如果不存在）
# with engine.connect() as conn:
#     conn.execute(text('''
#         CREATE TABLE IF NOT EXISTS books_data (
#             title STRING,
#             s_img STRING,
#             scrible STRING,
#             author STRING,
#             publisher STRING,
#             pub_date STRING,
#             price FLOAT,
#             score FLOAT,
#             num INT
#         )
#     '''))


df_cleaned.to_csv('../data/cleaned_books_data.csv', index=False)

# # 将清洗后的数据写入 Hive
# df_cleaned.to_sql('books_data', engine, if_exists='append', index=False)

print("数据清洗并存储完成，文件以保存至data/cleaned_books_data.csv")
