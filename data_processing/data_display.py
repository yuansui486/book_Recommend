from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 初始化 SparkSession 并连接到 Hive
spark = SparkSession.builder \
    .appName("DisplayRecommendationsWithFriendlyDisplay") \
    .config("spark.sql.warehouse.dir", "hdfs://192.168.128.130:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.128.130:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 1. 从 Hive 中读取推荐结果
print("从Hive中读取推荐结果...")
recommendations_df = spark.sql("SELECT * FROM default.user_recommendations")

# 查看user_recommendations表的结构
recommendations_df.printSchema()
recommendations_df.show(5)

# 2. 从 Hive 中读取书籍数据，获取书籍的书名、作者等信息
print("从Hive中读取书籍数据...")
books_df = spark.sql("SELECT id AS book_id, title, author, publisher FROM default.books_data")

# 3. 将推荐结果与书籍数据进行连接，获取书名、作者等信息
print("将推荐结果与书籍数据进行连接...")
recommendations_with_titles = recommendations_df \
    .select("user_id", "book_id", "rating") \
    .join(books_df, on="book_id", how="left")

# 4. 展示更加用户友好的推荐结果，包括书名、作者、评分等信息
print("用户友好的推荐结果展示：")
recommendations_with_titles.select("user_id", "title", "author", "publisher", "rating").show(10, truncate=False)

# 关闭 SparkSession
spark.stop()

print("推荐结果展示完成！")
