# 图书推荐系统

本项目实现了一个基于大数据技术的图书推荐系统，使用 Hive 和 Spark 进行数据处理、分析和推荐算法的实现。项目涵盖了数据清洗、分析、推荐算法的应用。推荐算法采用了 ALS（交替最小二乘法）协同过滤。

## 项目结构

```bash
/book_recommendation
├── /data_processing          # 数据处理与分析代码
│   ├── data_cleaning.py      # 数据清洗并存储到 CSV 文件
│   ├── data_analysis.py      # 从 Hive 读取数据并进行分析
│   ├── data_display.py       # 从 Hive 读取推荐结果并展示
│   └── /metastore_db         # Hive metastore 数据库
│       ├── book_users_create.py # 生成用户信息数据并保存到 CSV
│       └── user_behaviors_create.py # 生成用户行为数据并保存到 CSV
├── /recommendation           # 推荐算法实现
│   └── als_recommendation.py # 基于 ALS 协同过滤的推荐算法
├── /data                     # 数据存储
│   ├── book_users            # 存储用户信息的 CSV 文件
│   ├── user_behaviors        # 存储用户行为数据的 CSV 文件
│   ├── user_recommendations  # 存储用户推荐结果的 CSV 文件
│   ├── cleaned_books_data.csv # 清洗后的图书数据
│   └── data.json             # 爬取的原始图书数据
├── /results                  # 推荐结果存储
│   └── user_recommendations.csv # ALS 推荐结果（可选）
└── README.md                 # 项目说明
```

## 项目概述

本项目的目标是通过分布式数据存储和处理技术，开发一个基于协同过滤的图书推荐系统。我们利用 Hive 管理大规模数据集，并通过 Spark 实现高效的分布式计算。该推荐系统结合了基于 ALS（交替最小二乘法）的协同过滤算法，生成个性化推荐。

### 核心功能

1. **数据清洗与存储（CSV 文件）**：
   - `data_cleaning.py` 负责对原始爬取的数据进行清洗，去除无效数据并规范化格式。清洗后的数据存储在 CSV 文件中，后续可以导入 Hive 进行处理【131†source】。

2. **用户数据生成**：
   - `book_users_create.py` 随机生成用户信息并保存为 CSV 文件，供后续使用【134†source】。

3. **用户行为数据生成**：
   - `user_behaviors_create.py` 生成模拟的用户行为数据（浏览、点击、评分等），并将其保存为 CSV 文件【133†source】。

4. **ALS 协同过滤推荐**：
   - `als_recommendation.py` 基于用户行为数据，使用 Spark 的 ALS 算法实现个性化推荐，生成每个用户的图书推荐列表【132†source】。

5. **数据分析与可视化**：
   - `data_analysis.py` 使用 Spark 从 Hive 中读取数据，进行统计分析，并生成书籍的评分分布、价格分布等可视化图表【135†source】。

6. **推荐结果展示**：
   - `data_display.py` 通过 Spark 从 Hive 中读取推荐结果，将推荐结果与书籍信息进行连接，并以更加用户友好的方式展示推荐书籍名称、作者、评分等信息【132†source】。

### 项目流程

1. **数据清洗**：通过 Pandas 对爬取的原始数据进行清洗，处理缺失值和格式问题，清洗后的数据存储在 `cleaned_books_data.csv` 中。
2. **生成用户数据与行为数据**：通过 `book_users_create.py` 和 `user_behaviors_create.py` 生成模拟的用户数据和用户行为数据，分别保存为 CSV 文件。
3. **ALS 协同过滤推荐**：基于用户行为数据，使用 ALS 算法生成个性化推荐，推荐结果可以保存到 CSV 文件，也可以通过 Hive 进行展示。
4. **推荐结果展示**：通过 `data_display.py` 展示每个用户的推荐结果，包括书籍名称、作者、评分等信息。
5. **数据分析与可视化**：通过 `data_analysis.py` 对数据进行可视化分析，生成各种图表，帮助理解数据的分布特征。

---

通过本项目，结合 Hive 和 Spark 的分布式计算能力，能够处理大规模数据集，并生成个性化的图书推荐。

