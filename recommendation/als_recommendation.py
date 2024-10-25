from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.ml.recommendation import ALS

import os

# 设置 PySpark 使用的 Python 解释器路径
os.environ['PYSPARK_PYTHON'] = 'C:/Users/ASUS/.conda/envs/book_Recommend/python'


# 初始化 SparkSession 并连接到 Hive
spark = SparkSession.builder \
    .appName("ALSRecommendation") \
    .config("spark.sql.warehouse.dir", "hdfs://192.168.128.130:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.128.130:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 1. 从 Hive 中读取用户信息数据和用户行为数据
print("从Hive读取数据中...")
user_info_df = spark.sql("SELECT * FROM default.book_users")  # 表名更新为 book_users
user_behavior_df = spark.sql("SELECT * FROM default.user_behaviors")  # 表名更新为 user_behaviors

# 展示读取到的用户信息和行为数据
print("用户信息数据：")
user_info_df.show(5)
print("用户行为数据：")
user_behavior_df.show(5)

# 2. 数据预处理：只使用 'rating' 行为的数据
print("正在处理用户行为数据，只保留评分记录...")
rating_data = user_behavior_df.filter(col("action") == "rating").select("user_id", "book_id", "rating")
rating_data.show(10)

# 3. ALS模型训练与推荐
print("正在训练ALS模型...")
als = ALS(
    maxIter=20,                # 迭代次数
    regParam=0.1,              # 正则化参数
    userCol="user_id",         # 用户ID列
    itemCol="book_id",         # 书籍ID列
    ratingCol="rating",        # 评分列
    coldStartStrategy="drop"   # 解决冷启动问题
)

# 训练 ALS 模型
model = als.fit(rating_data)

# 为每个用户生成5个推荐的书籍
print("为每个用户生成5个推荐的书籍...")
user_recommendations = model.recommendForAllUsers(5)

# 展示部分推荐结果
print("推荐结果展示：")
user_recommendations.show(10, truncate=False)

# 4. 使用 explode 展开 recommendations 列，并将其存储为 CSV 文件
print("将推荐结果展开，并存储到本地路径 ../data/user_recommendations/，写成单个 CSV 文件...")
exploded_recommendations = user_recommendations.select(
    col("user_id"),
    explode(col("recommendations")).alias("recommendation")
).select(
    col("user_id"),
    col("recommendation.book_id"),
    col("recommendation.rating")
)

# 将展开的推荐结果合并为单个文件并保存
exploded_recommendations.coalesce(1).write.option("header", "true").mode("overwrite").csv("../data/user_recommendations")

print("推荐结果已成功存储到 ../data/user_recommendations/ 路径下")

# 关闭 SparkSession
spark.stop()

print("ALS协同过滤推荐完成！")