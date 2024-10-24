# 图书推荐系统

本项目实现了一个基于大数据技术的图书推荐系统，使用 Hive 和 Spark 进行数据处理、分析和推荐算法的实现。项目涵盖了数据清洗、分析、可视化，以及 K-means 聚类和 ALS 协同过滤推荐算法的应用。

## 项目结构

```bash
/book_recommendation
├── /data_processing          # 数据处理与分析代码
│   ├── data_cleaning.py      # 数据清洗并存储到 Hive
│   ├── data_analysis.py      # 从 Hive 读取数据并进行分析
│   └── data_visualization.py # 使用 Matplotlib 进行数据可视化
├── /recommendation           # 推荐算法实现
│   ├── kmeans_clustering.py  # K-means 聚类算法
│   ├── collaborative_filtering.py # ALS 协同过滤算法
├── /data                     # 数据存储
│   └── data.json             # 爬取的原始数据
├── /results                  # 推荐结果和聚类结果存储
│   └── user_recommendations.csv # ALS 推荐结果
└── README.md                 # 项目说明
```

## 项目概述

本项目的目标是使用分布式数据存储和处理技术，开发一个图书推荐系统。我们利用 Hive 管理大规模数据集，并通过 Spark 实现高效的分布式计算。该推荐系统结合了基于 K-means 聚类算法的图书分类推荐，以及基于 ALS（交替最小二乘法）的协同过滤算法，生成个性化推荐。

### 核心功能

1. **数据清洗与存储（Hive）**：
   - `data_cleaning.py` 负责对原始爬取的数据进行清洗，去除无效数据并规范化格式。清洗后的数据存储在 Hive 表中，方便后续处理。

2. **数据分析（Spark 和 Hive）**：
   - `data_analysis.py` 使用 Spark 从 Hive 中读取数据，并进行各种统计分析和聚合操作，帮助理解数据的整体特征。

3. **数据可视化**：
   - `data_visualization.py` 生成图书数据的可视化图表，例如价格分布和评分分布。由于 Spark 不直接支持 Matplotlib，可将 Spark DataFrame 转换为 Pandas DataFrame 后进行可视化处理。

4. **K-means 聚类推荐**：
   - `kmeans_clustering.py` 使用 Spark 的 K-means 算法对图书数据进行聚类，将相似图书划分为多个类别，基于聚类结果进行图书推荐。

5. **ALS 协同过滤推荐**：
   - `collaborative_filtering.py` 基于用户行为数据，使用 Spark 的 ALS 算法实现个性化推荐，生成每个用户的图书推荐列表。

6. **推荐结果展示**：
   - 推荐结果存储在 Hive 中，并通过终端或 CSV 文件的形式展示。用户可以查看个性化的图书推荐结果。

### 项目流程

1. **数据清洗**：通过 Pandas 对爬取的原始数据进行清洗，处理缺失值和格式问题，最终将数据存储到 Hive。
2. **数据读取**：通过 Spark 连接 Hive，读取存储的图书数据并进行后续分析和处理。
3. **数据分析与可视化**：使用 Spark 加载数据并进行基本分析，通过 Matplotlib 对数据进行可视化展示。
4. **K-means 聚类推荐**：利用 K-means 算法对图书进行聚类，将相似的图书分类，并基于聚类结果进行推荐。
5. **协同过滤推荐（ALS）**：基于用户行为数据，使用 ALS 算法生成个性化推荐，推荐结果存储在 Hive 中。
6. **推荐结果展示**：通过终端输出或导出 CSV 文件的方式展示推荐结果。


本项目通过结合 Hive 和 Spark 的优势，实现了高效的分布式数据处理和推荐系统，能够处理大规模数据并生成个性化推荐。
```