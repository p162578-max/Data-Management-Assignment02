# MovieLens 100K Spark 与 Cassandra 数据管理项目说明

## 1. 项目概述

本项目基于 MovieLens 100K 数据集完成一个 Python、Apache Spark 与 Cassandra 结合的数据处理与分析流程。项目使用的数据文件主要包括：

- `u.user`：用户信息，包括用户 ID、年龄、性别、职业和邮编。
- `u.data`：用户评分数据，包括用户 ID、电影 ID、评分和时间戳。
- `u.item`：电影信息，包括电影 ID、电影标题、上映日期、IMDb URL 和电影类型标记。

项目目标是将 MovieLens 原始数据从 HDFS 读取到 Spark 中，先创建 RDD，再转换为 DataFrame，之后完成数据清洗、分析查询，并将五个分析任务的结果写入 Cassandra 数据库。最后通过 `cqlsh` 使用 `SELECT` 查询结果表，验证 Spark 写入 Cassandra 的结果是否正确。

本项目完成的五个分析任务如下：

1. 计算每部电影的平均评分。
2. 找出平均评分最高的前 10 部电影。
3. 找出评分数量至少为 50 的用户，并判断其最喜欢的电影类型。
4. 找出所有年龄小于 20 岁的用户。
5. 找出职业为 `scientist` 且年龄在 30 到 40 岁之间的用户。

## 2. 环境要求

本项目在虚拟机环境中运行，涉及 HDFS、Apache Spark、Cassandra 和 Python 脚本。由于课程环境中的 Spark 版本较旧，运行时需要特别注意 Cassandra connector 的版本兼容问题。

建议环境如下：

| 组件 | 说明 |
|---|---|
| 操作环境 | Linux 虚拟机 |
| 数据存储 | HDFS |
| 分布式计算 | Apache Spark 2.3.0 |
| 数据库 | Apache Cassandra |
| 开发语言 | Python / PySpark |
| 远程连接工具 | PuTTY |
| 数据集 | MovieLens 100K |

本项目中较关键的版本兼容点是 Spark Cassandra Connector。由于 Spark 2.3.0 使用 Scala 2.11 生态，不能直接使用较新的 connector 版本。因此运行 `spark-submit` 时指定：

```bash
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.2 assignment2.py
```

同时，Python 代码中的 `SparkSession.builder` 不再重复配置：

```python
.config("spark.jars.packages", "...")
```

也就是说，需要将代码中这一行注释掉或删除，避免 connector 在代码中和提交命令中重复配置，造成版本冲突或依赖加载异常。

## 3. 项目运行流程

本项目的实际运行逻辑如下。

首先进入虚拟机，确认 MovieLens 数据集已经上传到 HDFS，并检查 `u.user`、`u.data` 和 `u.item` 三个文件的具体 HDFS 路径。Python 代码后续会通过这些路径读取原始数据。

然后使用 PuTTY 连接虚拟机环境，启动 Cassandra 服务，并进入 Cassandra 的命令行工具 `cqlsh`。在 `cqlsh` 中创建数据库 keyspace，本项目命名为 `movielens_ks`，用于保存后续 Spark 分析得到的结果表。

接着在 Cassandra 中预先创建 5 个空表，用来承接 Spark 程序运行后写入的五类分析结果：

- `movie_average_ratings`
- `top_ten_movies`
- `favourite_genres`
- `users_under_20`
- `scientists_30_to_40`

创建 Cassandra 表后，在 PuTTY 中创建 Python 文件，例如 `assignment2.py`，将 PySpark 代码写入该文件。代码完成后，通过 `spark-submit` 提交运行，并在命令中指定 Cassandra connector：

```bash
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.2 assignment2.py
```

程序运行后，Spark 会完成五个任务的计算，并将五个结果 DataFrame 分别写入 Cassandra 的 `movielens_ks` keyspace。最后再次进入 `cqlsh`，分别对五张结果表执行 `SELECT * FROM ... LIMIT 10;`，查看前 10 行结果，验证数据写入是否成功。

## 4. 代码分段解析

### 4.1 导入库与创建 SparkSession

代码首先导入 PySpark 所需模块，包括 `SparkSession`、常用 SQL 函数、数据类型定义和窗口函数。

`build_spark_session()` 函数用于创建 SparkSession，并配置 Cassandra 的连接地址：

```python
SparkSession.builder \
    .appName(APP_NAME) \
    .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
    .getOrCreate()
```

需要注意的是，本项目最终运行时没有在代码中配置 `spark.jars.packages`，而是在 `spark-submit` 命令中添加 `--packages` 参数。这是因为 Spark 2.3.0 对 connector 版本要求较严格，统一在提交命令中指定更稳定。

### 4.2 解析 MovieLens 原始文件

代码分别定义了三个解析函数：

- `parse_user()`：解析 `u.user`，字段分隔符为 `|`。
- `parse_rating()`：解析 `u.data`，字段分隔符为 tab。
- `parse_movie()`：解析 `u.item`，字段分隔符为 `|`。

其中 `u.item` 文件比较特殊，除了电影基本信息外，还包含 19 个电影类型字段，例如 `Action`、`Comedy`、`Drama`、`Sci-Fi` 等。每个类型字段使用 0 或 1 表示该电影是否属于该类型。

### 4.3 从 HDFS 创建 RDD

Spark 通过 `sparkContext.textFile()` 从 HDFS 读取三个原始文件：

```python
users_rdd = sc.textFile(...).map(parse_user)
ratings_rdd = sc.textFile(...).map(parse_rating)
movies_rdd = sc.textFile(...).map(parse_movie)
```

这一部分满足作业中“从原始数据集创建 RDD”的要求。RDD 是 Spark 的底层分布式数据结构，适合展示从文本文件到结构化数据的转换过程。

### 4.4 RDD 转换为 DataFrame

代码为三个数据集分别定义 schema，然后使用：

```python
spark.createDataFrame(rdd, schema)
```

将 RDD 转换为 Spark DataFrame。DataFrame 具有表结构，后续可以使用 Spark SQL 或 DataFrame API 进行分析。

转换后的主要 DataFrame 包括：

- `users_df`
- `ratings_df`
- `movies_df`

代码还使用 `dropna()` 删除关键字段为空的数据，属于基本的数据清洗步骤。

### 4.5 电影类型宽表转长表

MovieLens 的 `u.item` 中，电影类型是 19 个独立列。为了统计用户最常评分的电影类型，需要将这些 genre columns 转换为更适合分析的长表结构。

转换后的 `movie_genres_df` 大致结构为：

| movie_id | movie_title | genre |
|---|---|---|
| 1 | Toy Story (1995) | Animation |
| 1 | Toy Story (1995) | Children |
| 1 | Toy Story (1995) | Comedy |

这样一部电影可以对应多个 genre，后续才能和评分数据连接，统计每个用户评分过的 genre 次数。

### 4.6 任务一：计算每部电影平均评分

任务一将 `ratings_df` 与 `movies_df` 按 `movie_id` 连接，然后按电影分组：

```sql
GROUP BY movie_id, movie_title
```

计算：

- `average_rating`：平均评分；
- `rating_count`：评分数量。

该结果写入 Cassandra 表 `movie_average_ratings`。

### 4.7 任务二：平均评分最高的前 10 部电影

任务二基于电影评分数据计算平均分，并按照：

```sql
ORDER BY average_rating DESC, rating_count DESC, movie_title ASC
LIMIT 10
```

选出平均评分最高的前 10 部电影。该结果写入 Cassandra 表 `top_ten_movies`。

### 4.8 任务三：评分至少 50 部电影用户的最喜欢类型

任务三先统计每个用户评分的电影数量，筛选：

```python
rated_movie_count >= 50
```

然后将评分表与电影 genre 长表连接，统计每个用户对不同 genre 的评分次数。最后使用窗口函数 `row_number()`，为每个用户选出评分次数最多的 genre，作为该用户的 `favourite_genre`。

该结果写入 Cassandra 表 `favourite_genres`。

这里的“最喜欢类型”是根据用户评分频率判断，而不是根据某一类型的平均评分判断。也就是说，某个用户最常评分 Drama，则认为其 favourite genre 是 Drama。

### 4.9 任务四：年龄小于 20 岁的用户

任务四直接在用户表中筛选：

```sql
WHERE age < 20
```

结果写入 Cassandra 表 `users_under_20`。

### 4.10 任务五：30 到 40 岁的 scientist 用户

任务五筛选职业为 `scientist` 且年龄在 30 到 40 岁之间的用户：

```sql
WHERE occupation = 'scientist'
  AND age BETWEEN 30 AND 40
```

结果写入 Cassandra 表 `scientists_30_to_40`。

### 4.11 写入 Cassandra 与结果验证

五个分析结果生成后，代码使用 Spark Cassandra Connector 写入 Cassandra：

```python
df.write \
  .format("org.apache.spark.sql.cassandra") \
  .mode("append") \
  .options(table=table_name, keyspace=CASSANDRA_KEYSPACE) \
  .save()
```

运行结束后，通过 `cqlsh` 分别查询五张表的前 10 行，确认数据已经写入 Cassandra。

## 5. 代码运行结果分析

### 5.1 任务一结果：每部电影平均评分

Spark 输出中，任务一展示了部分电影的平均评分结果。例如：

| movie_id | movie_title | average_rating | rating_count |
|---|---|---:|---:|
| 1 | Toy Story (1995) | 3.878 | 452 |
| 2 | GoldenEye (1995) | 3.206 | 131 |
| 3 | Four Rooms (1995) | 3.033 | 90 |
| 4 | Get Shorty (1995) | 3.55 | 209 |
| 5 | Copycat (1995) | 3.302 | 86 |

从结果可以看出，不同电影的平均评分和评分人数差异较大。例如 `Toy Story (1995)` 的评分人数为 452，平均评分为 3.878，说明该电影在数据集中有较高的用户覆盖度，平均评价也较好。

Cassandra 查询 `movie_average_ratings` 表也成功返回了电影平均评分数据，说明任务一结果已经写入 Cassandra。

### 5.2 任务二结果：平均评分最高的前 10 部电影

Spark 输出显示，平均评分最高的前 10 部电影平均分均为 5.0，例如：

| movie_title | average_rating | rating_count |
|---|---:|---:|
| Prefontaine (1997) | 5.0 | 3 |
| Star Kid (1997) | 5.0 | 3 |
| Saint of Fort Washington, The (1993) | 5.0 | 2 |
| Santa with Muscles (1996) | 5.0 | 2 |
| Aiqing wansui (1994) | 5.0 | 1 |

从结果可以发现，虽然这些电影平均评分最高，但它们的 `rating_count` 普遍较低，有些电影只有 1 到 3 条评分。因此，这个排名反映的是“平均分最高”，但不一定代表最稳定或最受大众认可的电影。

如果要进行更稳健的推荐分析，可以进一步设置最低评分人数阈值，例如只统计评分人数大于 50 或 100 的电影。但本作业题目只要求找出平均评分最高的前 10 部电影，因此当前结果符合要求。

Cassandra 中 `top_ten_movies` 表的查询结果与 Spark 输出一致，说明写入和验证成功。

### 5.3 任务三结果：活跃用户最喜欢的电影类型

任务三结果展示了评分数量至少 50 的用户，以及每个用户最常评分的电影类型。例如 Spark 输出中：

| user_id | age | occupation | rated_movie_count | favourite_genre | genre_rating_count |
|---:|---:|---|---:|---|---:|
| 1 | 24 | technician | 272 | Drama | 107 |
| 2 | 53 | other | 62 | Drama | 35 |
| 3 | 23 | writer | 54 | Drama | 22 |
| 5 | 33 | other | 175 | Comedy | 82 |
| 6 | 42 | executive | 211 | Drama | 104 |

从结果可以看出，很多活跃用户最常评分的类型是 `Drama`。这可能说明在 MovieLens 100K 数据集中，Drama 类型电影数量较多，或者用户对 Drama 类型电影参与评分的频率较高。

Cassandra 中 `favourite_genres` 表也成功返回了结果。例如部分用户的 `favourite_genre` 为 `Drama` 或 `Action`，并且包含 `rated_movie_count` 和 `genre_rating_count`，说明该任务不仅完成了筛选，也完成了用户偏好类型统计。

### 5.4 任务四结果：年龄小于 20 岁的用户

任务四输出了年龄小于 20 岁的用户，例如：

| user_id | age | gender | occupation | zip_code |
|---:|---:|---|---|---|
| 30 | 7 | M | student | 55436 |
| 471 | 10 | M | student | 77459 |
| 289 | 11 | M | none | 94619 |
| 142 | 13 | M | other | 48118 |
| 609 | 13 | F | student | 55106 |

从结果可以看出，年龄小于 20 岁的用户大多是 `student`，这符合实际情况。该结果可用于后续分析年轻用户群体的评分偏好。

Cassandra 中 `users_under_20` 表也成功查询到前 10 行数据，说明筛选结果已正确写入。

### 5.5 任务五结果：30 到 40 岁的 scientist 用户

任务五输出了职业为 `scientist` 且年龄在 30 到 40 岁之间的用户，例如：

| user_id | age | gender | occupation | zip_code |
|---:|---:|---|---|---|
| 538 | 31 | M | scientist | 21010 |
| 730 | 31 | F | scientist | 32114 |
| 554 | 32 | M | scientist | 62901 |
| 183 | 33 | M | scientist | 27708 |
| 272 | 33 | M | scientist | 53706 |

结果说明 Spark 成功基于职业和年龄两个条件进行了组合筛选。Cassandra 查询 `scientists_30_to_40` 表也返回了对应数据，证明第五个任务结果已经成功存储。

## 6. Cassandra 查询验证

项目完成后，在 `cqlsh` 中分别执行以下查询语句进行验证：

```sql
SELECT * FROM movielens_ks.top_ten_movies LIMIT 10;
SELECT * FROM movielens_ks.movie_average_ratings LIMIT 10;
SELECT * FROM movielens_ks.favourite_genres LIMIT 10;
SELECT * FROM movielens_ks.users_under_20 LIMIT 10;
SELECT * FROM movielens_ks.scientists_30_to_40 LIMIT 10;
```

五个查询均返回了对应表的数据，说明 Spark 分析结果已经成功写入 Cassandra。截图中可以看到 Cassandra 表中包含 movie_id、average_rating、movie_title、rating_count、user_id、age、occupation、favourite_genre 等字段，字段内容与 Spark 程序输出一致。

## 7. 项目挑战

本项目中最主要的挑战是 Spark 与 Cassandra Connector 的版本兼容问题。

由于实验环境中使用的是 Spark 2.3.0，因此不能使用过新的 Spark Cassandra Connector。较新的 connector 可能依赖不同 Scala 版本或 Spark 版本，导致运行时出现 package 下载、类加载或 connector 初始化失败的问题。

解决方法是在 `spark-submit` 命令中明确指定与 Spark 2.3.0 更匹配的 connector：

```bash
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.2 assignment2.py
```

同时修改 `build_spark_session()`，注释掉或删除代码中的：

```python
.config("spark.jars.packages", ...)
```

这样可以避免同一个依赖在代码和提交命令中重复配置，也能让 Spark 在提交阶段统一加载 Cassandra connector。

另一个挑战是 MovieLens 的 `u.item` 文件中电影类型字段较多，并且采用宽表形式存储。为了完成 favourite genre 分析，需要将 19 个 genre columns 转换成长表，再与评分数据连接。这一步如果不处理好，用户最喜欢类型的统计会比较困难。

此外，Cassandra 查询结果默认不一定按照写入顺序显示，因此在 `cqlsh` 中使用 `LIMIT 10` 主要是为了验证数据存在和字段正确，而不是严格展示 Spark 中的排序顺序。对于排名结果，Spark 输出中的排序更适合作为最终分析展示。

## 8. 结论

本项目成功构建了一个基于 MovieLens 100K 数据集的 Spark 与 Cassandra 数据分析流程。项目从 HDFS 读取原始数据，使用 RDD 完成初步解析，再转换为 DataFrame 进行结构化处理，并通过 Spark SQL 和 DataFrame API 完成五个分析任务。

分析结果显示，MovieLens 数据集中不同电影的平均评分差异明显；平均评分最高的电影大多评分人数较少，因此解读排名时需要注意样本数量影响；在活跃用户的电影类型偏好中，Drama 和 Action 等类型出现较多；年轻用户主要集中在 student 群体；职业为 scientist 且年龄在 30 到 40 岁之间的用户也可以被准确筛选出来。

最终，五个分析结果均成功写入 Cassandra，并通过 `cqlsh` 查询得到验证。整个项目满足作业对 HDFS、RDD、DataFrame、Spark SQL、Cassandra 写入与查询验证的要求，也体现了从数据读取、处理、分析到数据库存储的完整数据管理流程。
