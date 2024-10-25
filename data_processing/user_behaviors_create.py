import random
import time
from pyspark.sql import SparkSession
from pyspark.sql import Row
import os

# 设置 PySpark 使用的 Python 解释器路径
os.environ['PYSPARK_PYTHON'] = 'C:/Users/ASUS/.conda/envs/book_Recommend/python'

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("GenerateUserBehaviorDataToCSV") \
    .getOrCreate()

# 假设已经有书籍ID列表
# 如果没有书籍ID，可以从现有文件或 Hive 中获取
book_ids = [i for i in range(1, 101)]  # 假设生成100本书籍的ID

# 生成用户行为数据函数
def generate_random_action():
    actions = ['view', 'click', 'rating']
    return random.choice(actions)

def generate_random_rating(action):
    if action == 'rating':
        return round(random.uniform(1.0, 5.0), 1)  # 如果是评分行为，生成评分
    else:
        return None  # 非评分行为不生成评分

# 生成5000条用户行为数据
user_behavior_data = []
for _ in range(5000):
    user_id = random.randint(1, 500)  # 随机用户ID
    book_id = random.choice(book_ids)  # 随机书籍ID
    action = generate_random_action()  # 随机生成用户行为（浏览、点击、评分）
    rating = generate_random_rating(action)  # 如果行为是评分，则生成评分
    event_timestamp = int(time.time())  # 当前时间戳

    user_behavior_data.append(Row(user_id=user_id, book_id=book_id, action=action, rating=rating, event_timestamp=event_timestamp))

# 创建用户行为的 DataFrame
user_behavior_df = spark.createDataFrame(user_behavior_data)

# 将用户行为数据保存为 CSV 文件
csv_behavior_output_path = '../data/user_behaviors/'

# 使用 coalesce(1) 确保数据写入到单个文件中
user_behavior_df.coalesce(1).write.csv(csv_behavior_output_path, mode='overwrite', header=True)

# 查看生成的用户行为数据
user_behavior_df.show(10)

# 关闭 Spark
spark.stop()
