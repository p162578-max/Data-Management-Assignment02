<h1 align='center'>Project Description</h1>

# 1. Project Overview

This project builds a data processing and analytics pipeline using the MovieLens 100K dataset with Python, Apache Spark, HDFS, and Cassandra. The main dataset files used in this project are:

- u.user: user information, including user ID, age, gender, occupation, and ZIP code.
- u.data: rating information, including user ID, movie ID, rating, and timestamp.
- u.item: movie information, including movie ID, movie title, release date, IMDb URL, and genre indicators.

The objective of this project is to read raw MovieLens data from HDFS into Spark, create RDDs, convert them into DataFrames, perform cleaning and analytical queries, and write the results into Cassandra tables. The final results are then verified in cqlsh by selecting records from the Cassandra tables.

The five analytical tasks completed in this project are:

1. Calculate the average rating for each movie.
2. Identify the top ten movies with the highest average ratings.
3. Identify users who rated at least 50 movies and determine their favourite movie genre.
4. Find all users who are less than 20 years old.
5. Find all users whose occupation is scientist and whose age is between 30 and 40 years old.

# 2. Environment Requirements

This project was executed in a virtual machine environment using HDFS, Apache Spark, Cassandra, and Python. Since the course environment uses an older Spark version, Cassandra connector compatibility is an important requirement.

The recommended environment is:

| Component | Description |
|---|---|
| Operating environment | Linux virtual machine |
| Data storage | HDFS |
| Distributed computing | Apache Spark 2.3.0 |
| Database | Apache Cassandra |
| Programming language | Python / PySpark |
| Remote access tool | PuTTY |
| Interactive notebook | Apache Zeppelin |
| Dataset | MovieLens 100K |

A key compatibility issue in this project is the Spark Cassandra Connector version. Since Spark 2.3.0 belongs to the Scala 2.11 ecosystem, newer connector versions may not work correctly. Therefore, the connector package was specified in the spark-submit command:

```bash
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.2 assignment2.py
```

At the same time, the following configuration was not repeated inside the Python code:

```python
.config("spark.jars.packages", "...")
```

In other words, this line was commented out or removed from build_spark_session() to avoid dependency conflicts or repeated connector configuration.

# 3. Project Execution Workflow

## 3.1 Cassandra 数据库配置与建表

### 3.1.1 Cassandra 激活与状态查询

在运行 Spark 任务之前，需要先确保 Cassandra 数据库已正常启动。具体步骤如下：

1. 通过 PuTTY 连接虚拟机，在终端中激活 Cassandra 服务。
2. 使用命令检查 Cassandra 激活状态

```bash
# 启动Cassandra服务
sudo service cassandra start

# 查看Cassandra服务运行状态
service cassandra status
```

![Cassandra 激活与状态查询](Putty%20screenshot/00_CassandraActivation%26StatusCheck.png)

### 3.1.2 CQL 建表脚本

Cassandra 正常运行后，进入 cqlsh 命令行界面，执行以下 CQL 脚本创建 keyspace 和所有需要的表：

```bash
CREATE KEYSPACE IF NOT EXISTS movielens_ks
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE movielens_ks;

CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY,
    age int,
    gender text,
    occupation text,
    zip_code text
);

CREATE TABLE IF NOT EXISTS ratings (
    user_id int,
    movie_id int,
    rating int,
    rating_timestamp int,
    PRIMARY KEY (user_id, movie_id)
);

CREATE TABLE IF NOT EXISTS movies (
    movie_id int PRIMARY KEY,
    movie_title text,
    release_date text,
    imdb_url text
);

CREATE TABLE IF NOT EXISTS movie_average_ratings (
    movie_id int PRIMARY KEY,
    movie_title text,
    average_rating double,
    rating_count bigint
);

CREATE TABLE IF NOT EXISTS top_ten_movies (
    movie_id int PRIMARY KEY,
    movie_title text,
    average_rating double,
    rating_count bigint
);

CREATE TABLE IF NOT EXISTS favourite_genres (
    user_id int PRIMARY KEY,
    age int,
    gender text,
    occupation text,
    rated_movie_count bigint,
    favourite_genre text,
    genre_rating_count bigint
);

CREATE TABLE IF NOT EXISTS users_under_20 (
    user_id int PRIMARY KEY,
    age int,
    gender text,
    occupation text,
    zip_code text
);

CREATE TABLE IF NOT EXISTS scientists_30_to_40 (
    user_id int PRIMARY KEY,
    age int,
    gender text,
    occupation text,
    zip_code text
);

TRUNCATE users;
TRUNCATE ratings;
TRUNCATE movies;
TRUNCATE movie_average_ratings;
TRUNCATE top_ten_movies;
TRUNCATE favourite_genres;
TRUNCATE users_under_20;
TRUNCATE scientists_30_to_40;
```
此脚本创建了：
- 三张原始数据表：users、ratings、movies（用于存储从 HDFS 加载的原始数据）。
- 五张结果表：分别对应五个分析任务的输出。
- 最后使用 TRUNCATE 清空所有表，确保每次运行 Spark 任务时都是干净的数据环境。

## 3.2 Spark代码实现

本项目采用了**两种方式**来实现五个任务分析，并将结果写入 Cassandra：

**方法一：通过 spark-submit 提交 Python 脚本**——将 PySpark 代码写成一个独立的 .py 文件，使用命令行提交运行。

**方法二：通过 Apache Zeppelin Notebook**——将代码分段落写入 Zeppelin 的交互式笔记本中，逐段执行并查看中间结果。

两种方法的代码逻辑一致，但运行方式和调试体验不同。但是不管哪种方法都必须先要在Cassandra中建立好keyspace，用于存储相应的分析表格

### 3.2.1 spark-submit 提交 Python 脚本

首先，我进入虚拟机并确认 MovieLens 数据集文件在 HDFS 中的准确路径，包括 u.user、u.data 和 u.item。这些 HDFS 路径随后在 Python 脚本中用于加载原始数据。

- "hdfs:/user/maria_dev/ml-100k"

然后，在 PuTTY 中创建 Assignment2.py 文件并写入 PySpark 代码，具体的代码我放在了 movielens_spark_cassandra_pipeline.py 文件中。保存.py文件后，使用 spark-submit 提交：

```bash
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.2 assignment2.py
```

注意: 因为我把py文件中的.config("spark.jars.packages", "...")语句注释掉了，在spark-submit中手动附加spark.jars.packages的指令，可以避免版本冲突，增加兼容。

程序运行成功后，Spark 完成五个分析任务，将结果 DataFrame 写入 Cassandra。最后，使用 cqlsh 查询语句验证数据是否正确写入

```bash
# 验证：查看所有 keyspace
DESCRIBE KEYSPACES;

# 切换到 movielens_ks 并列出其中的表
USE movielens_ks;

SELECT * FROM movielens_ks.movie_average_ratings LIMIT 10;
SELECT * FROM movielens_ks.top_ten_movies LIMIT 10;
SELECT * FROM movielens_ks.favourite_genres LIMIT 10;
SELECT * FROM movielens_ks.users_under_20 LIMIT 10;
SELECT * FROM movielens_ks.scientists_30_to_40 LIMIT 10;
```

### 3.2 方法二：Zeppelin Notebook 交互式执行

除了 spark-submit 方式，我还尝试在 Apache Zeppelin 中运行相同的 PySpark 代码。Zeppelin 提供了一个交互式的 Web 界面，可以将代码分成多个段落（paragraph），逐段执行并即时查看输出。

Zeppelin Notebook 的段落结构如下：

```bash
# Zeppelin 版本太老（HDP 0.7.x / Spark2），需要用"手动配置jar"

在 Zeppelin → Spark → Properties 里配置如下参数：

Key (name) : spark.jars.packages
Value: com.datastax.spark:spark-cassandra-connector_2.11:2.4.3

配置完成后点击save保存

依次操作：Interpreter → spark2 → Restart 重启Spark2解释器
```

- **段落 1 — Imports & Configuration**：导入所需的 PySpark 模块，配置 HDFS 基础路径和 Cassandra keyspace，并定义 19 个电影类型列。
  
- **段落 2 — Parse Functions & Schemas**：定义 parse_user()、parse_rating()、parse_movie() 三个解析函数以及对应的 Schema 结构。
  
- **段落 3 — Load Raw Data from HDFS**：从 HDFS 读取数据集，创建 RDD 并转换为 DataFrame，同时进行去空值清洗。
  
- **段落 4 — Task 1: Average Rating per Movie**：计算每部电影的平均评分。
  
- **段落 5 — Task 2: Top Ten Movies**：按平均评分排序，选出前十部电影。
  
- **段落 6 — Task 3: Favourite Genre (Users with ≥ 50 Ratings)**：找到评分数量不少于 50 部的活跃用户，并确定他们最喜欢的电影类型。
  
- **段落 7 — Task 4: Users Under 20 Years Old**：筛选年龄小于 20 岁的用户。
  
- **段落 8 — Task 5: Scientists Aged 30–40**：筛选职业为 scientist 且年龄在 30 到 40 岁之间的用户。
  
- **段落 9 — Write Results to Cassandra**：将五个任务的 DataFrame 写入对应的 Cassandra 表
  
- **段落 10 — Verify in Cassandra**：从 Cassandra 表中读取数据，验证写入是否正确。

Zeppelin 段落之间可以共享 SparkContext 和 SparkSession，因此前段创建好的 DataFrame 后段可以直接使用，大大提高了调试效率。


## 5. Code Section Explanation

### 5.1 Importing Libraries and Creating SparkSession

代码首先导入所需的 PySpark 模块，包括 SparkSession、SQL 函数、数据类型和窗口函数。

uild_spark_session() 函数用于创建 SparkSession，并配置 Cassandra 连接主机：

`python
SparkSession.builder \
    .appName(APP_NAME) \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .getOrCreate()
`

在本项目中，spark.jars.packages 没有在代码中配置。相反，Cassandra 连接器在 spark-submit 命令中指定。这种方式对 Spark 2.3.0 更稳定，可以避免连接器版本冲突。

### 5.2 Parsing the Raw MovieLens Files

代码定义了三个解析函数：

- parse_user()：解析 u.user，使用 | 作为分隔符。
- parse_rating()：解析 u.data，使用 Tab 作为分隔符。
- parse_movie()：解析 u.item，使用 | 分隔前 5 个字段，之后的 19 列为电影类型标志位。

同时定义了三个 Schema，分别对应三张 DataFrame。然后从 HDFS 中读取文件，创建 RDD 映射为 DataFrame，并使用 dropna() 进行基本清洗。

### 5.3 Task 1: Calculating Average Rating for Each Movie

使用 Spark DataFrame API 按 movie_id 分组，计算平均评分。将计算结果与 movies_df 关联，获取电影标题等额外信息。最终将结果写入 Cassandra 表 movie_average_ratings。

### 5.4 Task 2: Identifying Top Ten Movies by Average Rating

在任务 1 结果的基础上进行排序，使用 orderBy(col("average_rating").desc()) 降序排列，然后 limit(10) 选取前十部，写入 Cassandra 表 	op_ten_movies。

### 5.5 Task 3: Identifying Active Users' Favourite Genre

对于评分数量达到 50 部及以上的活跃用户，将电影类型列从宽表转换为长表（使用 explode 和 rray 函数）。计算每个用户在每个类型上的评分数量，最后使用 
ow_number() 窗口函数选出每个用户评分最多的类型作为最爱类型。结果写入 Cassandra 表 avourite_genres。

### 5.6 Task 4 and Task 5: Filtering Users by Age and Occupation

任务 4 和 5 是两个相对简单的条件过滤任务。任务 4 筛选年龄小于 20 岁的用户，写入 users_under_20。任务 5 筛选职业为 scientist 且年龄在 30 到 40 岁之间的用户，写入 scientists_30_to_40。

## 6. Results Analysis

### 6.1 Task 1 Result: Average Rating per Movie

任务 1 的计算结果表明，评分数量较少的电影容易获得极端评分（如 1.0 或 5.0），而热门电影的评分通常集中在 3 到 4 之间。在 Cassandra 中查询 movie_average_ratings 表中的前十条记录，结果与 Spark 输出完全一致，验证了数据写入的正确性。

### 6.2 Task 2 Result: Top Ten Movies

排名前十的电影平均评分均为 5.0，但评分数量大多只有 1 到 3 条。例如 "Great Day in Harlem, A (1994)" 只有 1 条评分。这表明高评分可能源于样本量过小，需要结合 
ating_count 字段谨慎解读排名结果。

### 6.3 Task 3 Result: Favourite Movie Genre for Active Users

任务 3 正确筛选出活跃用户并返回结果。输出中包含 
ated_movie_count 和 genre_rating_count，表明活跃用户过滤和最爱类型计算均正确完成。

### 6.4 Task 4 Result: Users Less Than 20 Years Old

任务 4 成功输出了年龄小于 20 岁的用户。结果显示大部分 20 岁以下用户为学生，这符合该年龄段的实际情况。Cassandra 表 users_under_20 的前十条数据也成功返回，证明写入正确。

### 6.5 Task 5 Result: Scientists Aged Between 30 and 40

任务 5 成功应用了职业和年龄两个过滤条件。Cassandra 表 scientists_30_to_40 中的查询返回了匹配记录，确认第五个任务的数据已正确完成并存储。

## 7. Cassandra Query Validation

完成 Spark 任务后，在 cqlsh 中针对五张结果表执行查询，验证数据是否正确写入：

`sql
SELECT * FROM movielens_ks.movie_average_ratings LIMIT 10;
SELECT * FROM movielens_ks.top_ten_movies LIMIT 10;
SELECT * FROM movielens_ks.favourite_genres LIMIT 10;
SELECT * FROM movielens_ks.users_under_20 LIMIT 10;
SELECT * FROM movielens_ks.scientists_30_to_40 LIMIT 10;
`

所有五条查询均从对应 Cassandra 表中返回了数据，截图如下：

| 截图 | 对应 Cassandra 表 |
|---|---|
| 03_CqlshSelect01.png | movie_average_ratings |
| 04_CqlshSelect02.png | 	op_ten_movies |
| 05_CqlshSelect03.png | avourite_genres |
| 06_CqlshSelect04.png | users_under_20 |
| 07_CqlshSelect05.png | scientists_30_to_40 |

截图中的字段（movie_id、verage_rating、movie_title、
ating_count、user_id、ge、occupation、avourite_genre 等）与 Spark 输出一致，证明结果已正确写入 Cassandra。

## 8. Project Challenges

### 8.1 Spark 与 Cassandra 连接器版本兼容性问题

本项目的第一个主要挑战是 Apache Spark 与 Spark Cassandra Connector 之间的版本兼容性。

由于实验环境使用的是 Spark 2.3.0（属于 Scala 2.11 生态），较新版本的 Spark Cassandra Connector 可能不兼容。新版本连接器可能依赖于不同的 Scala 或 Spark 版本，导致包下载错误、类加载错误或连接器初始化失败。

**解决方案**：在 spark-submit 命令中指定兼容的连接器版本，同时在代码中不重复配置 spark.jars.packages：

`ash
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.2 assignment2.py
`

`python
# build_spark_session() 中注释掉或移除此行，避免重复配置
# .config("spark.jars.packages", "...")
`

### 8.2 电影类型数据结构处理

第二个挑战是 u.item 文件中电影类型的存储结构。19 种电影类型以独立的列存储（每列 0 或 1），这种宽表格式不利于统计用户在各类型上的评分数量。

**解决方案**：使用 Spark 的 rray 和 explode 函数，将宽表转为长表后再与评分数据做关联计算，从而正确统计每个用户在每个类型上的评分数量，并找出最爱类型。

### 8.3 Zeppelin Notebook 的导入与使用

第三个挑战是在 Zeppelin 中导入和运行 Notebook。由于 Zeppelin 的 JSON 文件格式较为复杂，直接在 Web 界面中逐段编写代码效率较低。

**解决方案**：我从 Zeppelin 导出了一个 .json 文件，在本地编辑器中完成了代码编写和注释添加，然后通过 Zeppelin 的导入功能将整个 Notebook 重新加载。关键在于：

1. 先在 Zeppelin 中创建一个空白 Note，导出一个模板 JSON。
2. 在本地编辑器中按模板结构添加段落（paragraphs），包括 PySpark 代码和详细的中文注释。
3. 通过 Zeppelin Web UI 的 "Import Note" 功能将修改后的 JSON 重新导入。
4. 导入后在 Zeppelin 中逐段运行调试，确保所有段落按顺序正确执行。

这种方式将代码编写与执行环境分离，本地编辑更高效，同时保留了 Zeppelin 交互式执行的优势。最终生成的 Zeppelin Notebook JSON 文件（Assignment02.json）包含了完整的代码和注释，可以直接导入并运行。

### 8.4 Cassandra 查询结果排序问题

最后一个挑战是 Cassandra 查询结果的排序。Cassandra 是分布式数据库，SELECT 查询返回的结果不一定按照 Spark 输出的顺序排列。因此，在 cqlsh 中使用 LIMIT 10 主要用于验证数据存在性和表结构字段的正确性。对于基于排序的分析结果，以 Spark 的输出为准更合适。

## 9. Conclusion

本项目成功使用 Spark 和 Cassandra 实现了完整的 MovieLens 100K 数据分析管道。原始数据从 HDFS 加载，经 RDD 解析后转为 DataFrame，完成清洗、分析和 Cassandra 写入。

项目采用两种方式实现：
- **方法一（spark-submit）**：将 PySpark 代码写成独立脚本，通过命令行提交，适合生产环境批量执行。
- **方法二（Zeppelin Notebook）**：在交互式 Web 界面中逐段执行，方便调试和查看中间结果，适合开发和教学场景。

结果显示，平均评分最高的电影往往只有极少数评分，因此在解读排名时需谨慎。对于活跃用户，Drama 和 Action 等类型常作为最喜爱类型出现。20 岁以下用户以学生为主，职业为 scientist 且年龄在 30 到 40 岁之间的用户被成功识别。

五个分析结果均通过两种方式正确写入 Cassandra，并在 cqlsh 中完成验证。本次项目涵盖了 HDFS 数据加载、RDD 创建、DataFrame 转换、Spark SQL 分析、Cassandra 写入和结果验证的完整数据处理工作流。
