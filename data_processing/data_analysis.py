from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

# 初始化 SparkSession 并连接到 Hive
spark = SparkSession.builder \
    .appName("DataAnalysis") \
    .config("spark.sql.warehouse.dir", "hdfs://192.168.128.130:8020/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://192.168.128.130:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# 从 Hive 中读取书籍数据
books_df = spark.sql("SELECT * FROM default.books_data")

# 查看数据结构
books_df.printSchema()

# 统计分析：书籍评分的基本统计
books_df.describe('score').show()

# 统计分析：价格的分布情况
books_df.describe('price').show()

# 统计分析：评分人数的分布情况
books_df.describe('num').show()

# 转换为Pandas DataFrame
books_pd_df = books_df.toPandas()
# 关闭 Spark
spark.stop()

# 绘制评分的分布情况
# 评分分布图（范围8.5到9.5）
plt.figure(figsize=(10, 6))
plt.hist(books_pd_df['score'].dropna(), bins=10, range=(8.5, 9.5), edgecolor='black')
plt.title('书籍评分分布（8.5-9.5）')
plt.xlabel('评分')
plt.ylabel('频率')
plt.grid(True)
plt.show()

# 价格分布图
plt.figure(figsize=(10, 6))
plt.hist(books_pd_df['price'].dropna(), bins=10, edgecolor='black')
plt.title('书籍价格分布')
plt.xlabel('价格（元）')
plt.ylabel('频率')
plt.grid(True)
plt.show()

# 评分人数分布图
plt.figure(figsize=(10, 6))
plt.hist(books_pd_df['num'].dropna(), bins=10, edgecolor='black')
plt.title('评分人数分布')
plt.xlabel('评分人数')
plt.ylabel('频率')
plt.grid(True)
plt.show()