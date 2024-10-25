from sqlalchemy import create_engine, text
import pandas as pd

# 使用SQLAlchemy连接Hive
engine = create_engine('hive://root@192.168.128.130:10000/default')

# 测试查询并读取数据到pandas DataFrame
try:
    with engine.connect() as conn:
        query = "SELECT 1"
        df = pd.read_sql(query, conn)
        print("查询成功，结果为：")
        print(df)
except Exception as e:
    print("连接或查询失败:", str(e))
# 连接 Hive
# engine = create_engine('hive://root@192.168.128.130:10000/default')

# 创建 Hive 表（如果不存在）
with engine.connect() as conn:
    conn.execute(text('''
        CREATE TABLE IF NOT EXISTS books_data (
            title STRING,
            s_img STRING,
            scrible STRING,
            author STRING,
            publisher STRING,
            pub_date STRING,
            price FLOAT,
            score FLOAT,
            num INT
        )
    '''))