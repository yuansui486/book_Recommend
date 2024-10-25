import random
import string
from pyspark.sql import SparkSession
from pyspark.sql import Row
import os

# 设置 PySpark 使用的 Python 解释器路径
os.environ['PYSPARK_PYTHON'] = 'C:/Users/ASUS/.conda/envs/book_Recommend/python'

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("GenerateUserDataToCSV") \
    .getOrCreate()

# 随机生成用户数据函数
def generate_random_username():
    return ''.join(random.choices(string.ascii_lowercase, k=8))

def generate_random_email(username):
    domains = ["@example.com", "@mail.com", "@test.com"]
    return username + random.choice(domains)

def generate_random_age():
    return random.randint(18, 60)

def generate_random_gender():
    return random.choice(["Male", "Female", "Non-binary"])

# 生成500条用户数据
user_data = []
for user_id in range(1, 501):
    username = generate_random_username()
    email = generate_random_email(username)
    age = generate_random_age()
    gender = generate_random_gender()

    user_data.append(Row(user_id=user_id, username=username, email=email, age=age, gender=gender))

# 创建用户信息 DataFrame
users_df = spark.createDataFrame(user_data)

# 将用户信息数据保存为 CSV 文件
# 设置文件保存路径
csv_output_path = '../data/book_users/'

# 使用 coalesce(1) 确保数据写入到单个文件中
users_df.coalesce(1).write.csv(csv_output_path, mode='overwrite', header=True)

# 查看生成的用户数据
users_df.show(10)

# 关闭 Spark
spark.stop()
