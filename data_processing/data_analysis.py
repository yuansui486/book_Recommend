from pyspark.sql import SparkSession

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

# 关闭 Spark
spark.stop()

