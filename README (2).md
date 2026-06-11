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

<div align="center">
    
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
    
</div>

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

## 3.1 Cassandra Database Configuration and Table Creation

### 3.1.1 Cassandra Activation and Status Check

Before running Spark tasks, it is necessary to ensure that the Cassandra database has been started normally. The specific steps are as follows:

1. Connect to the virtual machine via PuTTY and activate the Cassandra service in the terminal.
2. Use the following commands to check the Cassandra activation status

```bash
# Start the Cassandra service
sudo service cassandra start

# Check the Cassandra service running status
service cassandra status
```
<div align="center">
    <img src="Putty%20screenshot/00_CassandraActivation%26StatusCheck.png">
</div>

### 3.1.2 CQL Table Creation Script

After Cassandra is running normally, enter the cqlsh command line interface and execute the following CQL script to create the keyspace and all required tables:

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
This script created:
- Three raw data tables: users, ratings, movies (used to store the raw data loaded from HDFS).
- Five result tables: Corresponding to the output of the five analysis tasks respectively.
- Finally, TRUNCATE is used to clear all tables, ensuring a clean data environment each time the Spark task is run.
- Use ''DESCRIBE KEYSPACES''

## 3.2 Spark Code Implementation

This project adopted **two methods** to implement the five task analyses and write the results to Cassandra:

**Method 1: Submit Python script via spark-submit** — Write the PySpark code into an independent .py file and submit it via the command line for execution.

**Method 2: Via Apache Zeppelin Notebook** — Write the code in paragraphs within Zeppelin's interactive notebook, executing paragraph by paragraph and viewing intermediate results.

The code logic of both methods is consistent, but the execution approach and debugging experience differ. However, regardless of which method, the keyspace must first be created in Cassandra to store the corresponding analysis tables.

### 3.2.1 Submitting Python Script via spark-submit

First, I entered the virtual machine and confirmed the exact HDFS path of the MovieLens dataset files as "hdfs:/user/maria_dev/ml-100k", including u.user, u.data, and u.item. These HDFS paths were then used in the Python script to load the raw data.

Then, I created the Assignment2.py file in PuTTY and wrote the PySpark code. The specific code is placed in the movielens_spark_cassandra_pipeline.py file. After saving the .py file, I submitted it using spark-submit:

```bash
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.2 assignment2.py
```

Note: Because I commented out the .config("spark.jars.packages", "...") statement in the .py file, and manually added the spark.jars.packages directive in spark-submit, this can avoid version conflicts and increase compatibility.

After the program ran successfully, Spark completed the five analytical tasks and wrote the result DataFrames to Cassandra. Finally, cqlsh query statements were used to verify whether the data was written correctly.

```bash
# Verify: View all keyspaces
DESCRIBE KEYSPACES;

# Switch to movielens_ks and list the tables in it
USE movielens_ks;

SELECT * FROM movielens_ks.movie_average_ratings LIMIT 10;
SELECT * FROM movielens_ks.top_ten_movies LIMIT 10;
SELECT * FROM movielens_ks.favourite_genres LIMIT 10;
SELECT * FROM movielens_ks.users_under_20 LIMIT 10;
SELECT * FROM movielens_ks.scientists_30_to_40 LIMIT 10;
```

### 3.2.2 Method 2: Zeppelin Notebook Interactive Execution

In addition to the spark-submit approach, I also attempted to run the same PySpark code in Apache Zeppelin. Zeppelin provides an interactive web interface where code can be divided into multiple paragraphs, executed step by step, and intermediate results can be viewed instantly.

The paragraph structure of the Zeppelin Notebook is as follows:

```bash
# The Zeppelin version is too old (HDP 0.7.x / Spark2), requiring "manual jar configuration"

In Zeppelin -> Spark -> Properties, configure the following parameters:

Key (name) : spark.jars.packages
Value: com.datastax.spark:spark-cassandra-connector_2.11:2.4.3

After configuration, click save to save it.

Proceed in order: Interpreter -> spark2 -> Restart to restart the Spark2 interpreter.
```

- **Paragraph 1 — Imports & Configuration**: Import the required PySpark modules, configure the HDFS base path and Cassandra keyspace, and define the 19 movie genre columns.
  
- **Paragraph 2 — Parse Functions & Schemas**: Define the three parsing functions — parse_user(), parse_rating(), parse_movie() — and the corresponding schema structures.
  
- **Paragraph 3 — Load Raw Data from HDFS**: Read the dataset from HDFS, create RDDs, convert them to DataFrames, and perform null value cleaning.
  
- **Paragraph 4 — Task 1: Average Rating per Movie**: Calculate the average rating for each movie.
  
- **Paragraph 5 — Task 2: Top Ten Movies**: Sort by average rating and select the top ten movies.
  
- **Paragraph 6 — Task 3: Favourite Genre (Users with >= 50 Ratings)**: Find active users who have rated at least 50 movies and determine their favourite movie genre.
  
- **Paragraph 7 — Task 4: Users Under 20 Years Old**: Filter users who are less than 20 years old.
  
- **Paragraph 8 — Task 5: Scientists Aged 30-40**: Filter users whose occupation is scientist and whose age is between 30 and 40 years old.
  
- **Paragraph 9 — Write Results to Cassandra**: Write the five task result DataFrames to the corresponding Cassandra tables.
  
- **Paragraph 10 — Verify in Cassandra**: Read data from the Cassandra tables to verify that the writes are correct.

Zeppelin paragraphs can share the SparkContext and SparkSession, so DataFrames created in earlier paragraphs can be directly used in later ones, greatly improving debugging efficiency.


# 4. Code Section Explanation

## 4.1 Importing Libraries and Creating SparkSession

- The code first imports the required PySpark modules, including SparkSession, SQL functions, data types, and window functions.

  ```bash
  from pyspark.sql import SparkSession
  from pyspark.sql.functions import (
    array,
    avg,
    col,
    count,
    explode,
    lit,
    row_number,
    struct,
  )
  from pyspark.sql.types import IntegerType, StringType, StructField, StructType
  from pyspark.sql.window import Window
  ```

- The build_spark_session() function is used to create a SparkSession and configure the Cassandra host:

  ```bash
  SparkSession.builder \
      .appName(APP_NAME) \
      .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
      .getOrCreate()
  ```

- Note: In this project, spark.jars.packages was not configured in the code. Instead, the Cassandra connector was specified in the spark-submit command. This approach is more stable for Spark 2.3.0 as it avoids connector version conflicts.

## 4.2 Parsing the Raw MovieLens Files

The code defines three parsing functions:

- parse_user(): Parses u.user, using | as the delimiter.
- parse_rating(): Parses u.data, using tab as the delimiter.
- parse_movie(): Parses u.item, using | to separate the first 5 fields, with the following 19 columns being movie genre flag bits.

It also defines three schemas corresponding to the three DataFrames. The files are then read from HDFS, RDDs are created and mapped to DataFrames, and dropna() is used for basic cleaning.

## 4.3 Implementation of the Five Analysis Tasks

- Task 1: Calculating Average Rating for Each Movie

 ```bash
# Task i: Average rating for each movie
    movie_average_ratings_df = spark.sql(
        """
        SELECT
            m.movie_id,
            m.movie_title,
            ROUND(AVG(r.rating), 3) AS average_rating,
            COUNT(*) AS rating_count
        FROM ratings r
        JOIN movies m
            ON r.movie_id = m.movie_id
        GROUP BY m.movie_id, m.movie_title
        ORDER BY m.movie_id
        """
    )
  ```

Using the Spark DataFrame API, group by movie_id and calculate the average rating. The result is joined with movies_df to obtain additional information such as movie titles. Finally, the result is written to the Cassandra table movie_average_ratings.

- Task 2: Identifying Top Ten Movies by Average Rating

 ```bash
  # Task ii: Top ten movies with the highest average ratings
    top_ten_movies_df = spark.sql(
        """
        SELECT
            m.movie_id,
            m.movie_title,
            ROUND(AVG(r.rating), 3) AS average_rating,
            COUNT(*) AS rating_count
        FROM ratings r
        JOIN movies m
            ON r.movie_id = m.movie_id
        GROUP BY m.movie_id, m.movie_title
        ORDER BY average_rating DESC, rating_count DESC, movie_title ASC
        LIMIT 10
        """
    )
  ```

Based on the Task 1 result, sort using orderBy(col("average_rating").desc()) in descending order, then limit(10) to select the top ten, and write to the Cassandra table top_ten_movies.

- Task 3: Identifying Active Users' Favourite Genre

  ```bash
    # Task iii: Active users who rated at least 50 movies and their favourite genre
    active_users_df = (
        ratings_df.groupBy("user_id")
        .agg(count("*").alias("rated_movie_count"))
        .filter(col("rated_movie_count") >= 50)
    )

    user_genre_counts_df = (
        ratings_df.join(movie_genres_df, "movie_id")
        .join(active_users_df, "user_id")
        .groupBy("user_id", "genre", "rated_movie_count")
        .agg(count("*").alias("genre_rating_count"))
    )

    genre_rank_window = Window.partitionBy("user_id").orderBy(
        col("genre_rating_count").desc(), col("genre").asc()
    )

    favourite_genres_df = (
        user_genre_counts_df.withColumn("genre_rank", row_number().over(genre_rank_window))
        .filter(col("genre_rank") == 1)
        .join(users_df, "user_id")
        .select(
            "user_id",
            "age",
            "gender",
            "occupation",
            "rated_movie_count",
            col("genre").alias("favourite_genre"),
            "genre_rating_count",
        )
        .orderBy("user_id")
    )
  ```

For users who have rated 50 or more movies (active users), transform the movie genre columns from wide format to long format (using the explode and array functions). Count the number of ratings per user per genre, then use the row_number() window function to identify each user's most-rated genre as their favourite genre.

- Task 4 and Task 5: Filtering Users by Age and Occupation

 ```bash
   # Task iv: Users under 20 years old
    users_under_20_df = spark.sql(
        """
        SELECT user_id, age, gender, occupation, zip_code
        FROM users
        WHERE age < 20
        ORDER BY age ASC, user_id ASC
        """
    )

    # Task v: Scientist users aged between 30 and 40
    scientists_30_to_40_df = spark.sql(
        """
        SELECT user_id, age, gender, occupation, zip_code
        FROM users
        WHERE occupation = 'scientist'
          AND age BETWEEN 30 AND 40
        ORDER BY age ASC, user_id ASC
        """
    )
  ```

Tasks 4 and 5 are two conditional filtering tasks. Task 4 filters users younger than 20 and writes them to users_under_20. Task 5 filters users whose occupation is scientist and whose age is between 30 and 40, and writes them to scientists_30_to_40.

## 4.4 Writing to Cassandra

 ```bash
    write_to_cassandra(users_df, "users")
    write_to_cassandra(ratings_df, "ratings")
    write_to_cassandra(movies_df.select("movie_id", "movie_title", "release_date", "imdb_url"), "movies")
    write_to_cassandra(movie_average_ratings_df, "movie_average_ratings")
    write_to_cassandra(top_ten_movies_df, "top_ten_movies")
    write_to_cassandra(favourite_genres_df, "favourite_genres")
    write_to_cassandra(users_under_20_df, "users_under_20")
    write_to_cassandra(scientists_30_to_40_df, "scientists_30_to_40")
  ```

- The core code from sections 4.3 and 4.4 above is encapsulated in the main() function, allowing a complete MovieLens data analysis task to be run in one go. Only the single entry point main() is exposed externally, requiring no parameters to be passed in — very concise.

## 4.5 Results Analysis

- Task 1 Result: Average Rating per Movie

Task 1's calculation results show that movies with fewer ratings tend to receive extreme ratings (such as 1.0 or 5.0), while popular movies' ratings typically cluster between 3 and 4. Querying the first ten records in the movie_average_ratings table in Cassandra, the results are fully consistent with the Spark output, verifying the correctness of the data write.

- Task 2 Result: Top Ten Movies

The top ten movies by average rating all scored 5.0, but most have only 1 to 3 ratings. For example, "Great Day in Harlem, A (1994)" has only 1 rating. This indicates that high ratings may stem from insufficient sample size, and the rating_count field should be considered when interpreting ranking results.

- Task 3 Result: Favourite Movie Genre for Active Users

Task 3 correctly filtered active users and returned results. The output includes rated_movie_count and genre_rating_count, indicating that both the active user filtering and favourite genre calculation were completed correctly.

- Task 4 Result: Users Less Than 20 Years Old

Task 4 successfully output users under 20 years old. The results show that most users under 20 are students, which is consistent with the real-world demographics of this age group.

- Task 5 Result: Scientists Aged Between 30 and 40

Task 5 successfully applied both the occupation and age filtering conditions.

- The overall output results are shown in the screenshots below

<div align="center">
    <img src="Putty%20screenshot/01_task1_3.png">
</div>

<div align="center">
    <img src="Putty%20screenshot/02_task4_5.png">
</div>

# 5. Cassandra Query Validation

After completing the Spark tasks, queries were executed against the five result tables in cqlsh to verify whether the data was written correctly:

```
SELECT * FROM movielens_ks.movie_average_ratings LIMIT 10;
SELECT * FROM movielens_ks.top_ten_movies LIMIT 10;
SELECT * FROM movielens_ks.favourite_genres LIMIT 10;
SELECT * FROM movielens_ks.users_under_20 LIMIT 10;
SELECT * FROM movielens_ks.scientists_30_to_40 LIMIT 10;
```

All five queries returned data from the corresponding Cassandra tables, and the screenshots are as follows:
<div align="center">

| Screenshot | Corresponding Cassandra Table |
|---|---|
| 03_CqlshSelect01.png | movie_average_ratings |
| 04_CqlshSelect02.png | top_ten_movies |
| 05_CqlshSelect03.png | favourite_genres |
| 06_CqlshSelect04.png | users_under_20 |
| 07_CqlshSelect05.png | scientists_30_to_40 |

</div>

<div align="center">
    <img src="Putty%20screenshot/03_CqlshSelect01.png">
    <img src="Putty%20screenshot/04_CqlshSelect02.png">
    <img src="Putty%20screenshot/05_CqlshSelect03.png">
    <img src="Putty%20screenshot/06_CqlshSelect04.png">
    <img src="Putty%20screenshot/07_CqlshSelect05.png">
</div>

The fields in the screenshots (movie_id, average_rating, movie_title, rating_count, user_id, age, occupation, favourite_genre, etc.) are consistent with the Spark output, confirming that the results have been correctly written to Cassandra.

# 6. Zeppelin Notebook

Another code logic, the core is the %spark2.pyspark loading for each cell. The other code is similar to the main body of the movielens_spark_cassandra_pipeline.py file. The runtime environment has been shifted from the PuTTY terminal to the Zeppelin notebook. I executed the code cell by cell in sequence and saved it as a JSON file. In this project's Assignment02.json file, it can be directly imported into Zeppelin for execution. Note that jars need to be manually configured. Partial result screenshots:

<div align="center">
    <img src="Zeppelin%20screenshot/10_Write%20to%20Cassandra.png">
</div>

<div align="center">
    <img src="Zeppelin%20screenshot/12_Verify%20Result1.png">
</div>

<div align="center">
    <img src="Zeppelin%20screenshot/13_Verify%20Result2.png">
</div>

# 7. Project Challenges

## 7.1 Spark and Cassandra Connector Version Compatibility Issue

The first major challenge in this project was the version compatibility between Apache Spark and the Spark Cassandra Connector.

Since the experimental environment uses Spark 2.3.0 (which belongs to the Scala 2.11 ecosystem), newer versions of the Spark Cassandra Connector may not be compatible. Newer connector versions may depend on different Scala or Spark versions, leading to package download errors, class loading errors, or connector initialization failures.

Solution 1: Specify a compatible connector version in the spark-submit command, while not duplicating the spark.jars.packages configuration in the code:

```bash
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.2 assignment2.py
```

```bash
# Comment out or remove this line in build_spark_session() to avoid duplicate configuration

# .config("spark.jars.packages", "...")
```

Solution 2: In Zeppelin -> Spark -> Properties, configure the following parameters:
```bash
Key (name) : spark.jars.packages
Value: com.datastax.spark:spark-cassandra-connector_2.11:2.4.3
```

After configuration, click save to save it, then proceed in order: Interpreter -> spark2 -> Restart to restart the Spark2 interpreter.

## 7.2 Movie Genre Data Structure Handling

The second challenge was the storage structure of movie genres in the u.item file. The 19 movie genres are stored as independent columns (each column being 0 or 1), and this wide-table format is not convenient for counting the number of ratings per user per genre.

**Solution**: Use Spark's array and explode functions to transform the wide table into a long table before joining with the rating data, thereby correctly counting the number of ratings per user per genre and identifying the favourite genre.


## 7.3 Cassandra Query Result Ordering Issue

The last challenge was the ordering of Cassandra query results. Cassandra is a distributed database, and the results returned by SELECT queries do not necessarily appear in the same order as the Spark output. Therefore, using LIMIT 10 in cqlsh is mainly for verifying the existence of data and the correctness of table structure fields. For ranking-based analysis results, it is more appropriate to refer to the Spark output.

## 8. Conclusion

This project successfully implemented a complete MovieLens 100K data analysis pipeline using Spark and Cassandra. Raw data was loaded from HDFS, parsed via RDD, converted to DataFrame, cleaned, analyzed, and written to Cassandra. This project covered the complete data processing workflow of HDFS data loading, RDD creation, DataFrame transformation, Spark SQL analysis, Cassandra writing, and result verification.

The project was implemented using two methods:

- **Method 1 (spark-submit)**: Write the PySpark code as an independent script and submit it via the command line, suitable for batch execution in production environments.
  
- **Method 2 (Zeppelin Notebook)**: Execute step by step in an interactive web interface, convenient for debugging and viewing intermediate results, suitable for development scenarios.

The results show that movies with the highest average ratings often have very few ratings, so caution is needed when interpreting rankings. For active users, genres such as Drama and Action frequently appear as favourite genres. Users under 20 are mostly students, and users whose occupation is scientist and whose age is between 30 and 40 were successfully identified. All five analytical results were correctly written to Cassandra through both methods and verified in cqlsh.
