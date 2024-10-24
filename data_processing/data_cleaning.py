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

def parse_pub_date(date_str):
    try:
        # 尝试完整格式 YYYY-MM-DD
        return pd.to_datetime(date_str, format='%Y-%m-%d', errors='raise')
    except:
        try:
            # 如果失败，尝试 YYYY-MM
            return pd.to_datetime(date_str, format='%Y-%m', errors='raise')
        except:
            try:
                # 如果失败，尝试 YYYY
                return pd.to_datetime(date_str, format='%Y', errors='coerce')
            except:
                return pd.NaT  # 如果解析失败，返回 NaT

# 应用日期解析函数
df['pub_date'] = df['pub_date'].apply(parse_pub_date)


# 4. 删除在重要列中存在缺失值的行
df_cleaned = df.dropna(subset=['title', 'author','pub_date' ,'price', 'score'])

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', 100)

print("清洗后的数据示例：")
print(df_cleaned.head(100))

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
print("处理后的数据行数：", df.shape[0])
print("清洗后的数据行数：", df_cleaned.shape[0])

# # 将清洗后的数据写入 Hive
# df_cleaned.to_sql('books_data', engine, if_exists='append', index=False)

print("数据清洗并存储完成，文件以保存至data/cleaned_books_data.csv")
