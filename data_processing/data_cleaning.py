import pandas as pd
from sqlalchemy import create_engine

# 读取爬取到的图书数据
df = pd.read_json('../data/data.json',lines=True)

# 数据清洗
# 1. 去除价格中的'元'字符，转换为浮点数
df['price'] = pd.to_numeric(df['price'].str.replace('元', ''), errors='coerce')

# 2. 将评分字段转换为浮点型数据
df['score'] = pd.to_numeric(df['score'], errors='coerce')

# 3. 将出版日期转换为日期格式
df['pub_date'] = pd.to_datetime(df['pub_date'], errors='coerce')

# 4. 去除缺失值和重复数据
df = df.dropna(subset=['title', 'author', 'price', 'score'])

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', 100)

print("清洗后的数据示例：")
print(df.head(10))

# # 连接 Hive
# engine = create_engine('hive://username@hive-server:10000/default')
#
# # 创建 Hive 表（如果不存在）
# with engine.connect() as conn:
#     conn.execute('''
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
#     ''')
#
# # 将清洗后的数据写入 Hive
# df.to_sql('books_data', engine, if_exists='append', index=False)
#
# print("数据清洗并存储到Hive完成")
