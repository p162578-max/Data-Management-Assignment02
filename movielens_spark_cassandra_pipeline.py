# coding=utf-8

"""
Assignment 02 - MovieLens 100k Spark + Cassandra Pipeline

This script is designed to be converted into a Jupyter Notebook or run with spark-submit.
It uses u.user, u.data, and u.item from the MovieLens 100k dataset.
"""

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


APP_NAME = "Assignment02_MovieLens_Spark_Cassandra"
HDFS_BASE_PATH = "hdfs:/user/maria_dev/ml-100k"
CASSANDRA_HOST = "127.0.0.1"
CASSANDRA_KEYSPACE = "movielens_ks"

GENRE_COLUMNS = [
    "unknown",
    "Action",
    "Adventure",
    "Animation",
    "Children",
    "Comedy",
    "Crime",
    "Documentary",
    "Drama",
    "Fantasy",
    "Film-Noir",
    "Horror",
    "Musical",
    "Mystery",
    "Romance",
    "Sci-Fi",
    "Thriller",
    "War",
    "Western",
]


def build_spark_session():
    return (
        SparkSession.builder.appName(APP_NAME)
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.11:2.5.2")
        .getOrCreate()
    )


def parse_user(line):
    # u.user format: user id | age | gender | occupation | zip code
    parts = line.split("|")
    return (
        int(parts[0]),
        int(parts[1]),
        parts[2],
        parts[3],
        parts[4],
    )


def parse_rating(line):
    # u.data format: user id, item id, rating, timestamp separated by tabs
    parts = line.split("\t")
    return (
        int(parts[0]),
        int(parts[1]),
        int(parts[2]),
        int(parts[3]),
    )


def parse_movie(line):
    # u.item is pipe-separated and movie title may contain spaces.
    parts = line.split("|")
    # 构建 genre 标志列表（从索引5到23）
    genre_flags = [int(value) for value in parts[5:24]]
    return (
        int(parts[0]),
        parts[1],
        parts[2],
        parts[3],
        parts[4],
    ) + tuple(genre_flags)


def create_dataframes_from_hdfs(spark):
    sc = spark.sparkContext

    users_rdd = sc.textFile("{}/u.user".format(HDFS_BASE_PATH)).map(parse_user)
    ratings_rdd = sc.textFile("{}/u.data".format(HDFS_BASE_PATH)).map(parse_rating)
    movies_rdd = sc.textFile("{}/u.item".format(HDFS_BASE_PATH)).map(parse_movie)

    users_schema = StructType(
        [
            StructField("user_id", IntegerType(), False),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("occupation", StringType(), True),
            StructField("zip_code", StringType(), True),
        ]
    )

    ratings_schema = StructType(
        [
            StructField("user_id", IntegerType(), False),
            StructField("movie_id", IntegerType(), False),
            StructField("rating", IntegerType(), True),
            StructField("rating_timestamp", IntegerType(), True),
        ]
    )

    movies_schema = StructType(
        [
            StructField("movie_id", IntegerType(), False),
            StructField("movie_title", StringType(), True),
            StructField("release_date", StringType(), True),
            StructField("video_release_date", StringType(), True),
            StructField("imdb_url", StringType(), True),
        ] + [StructField(genre, IntegerType(), True) for genre in GENRE_COLUMNS]
    )

    users_df = spark.createDataFrame(users_rdd, users_schema)
    ratings_df = spark.createDataFrame(ratings_rdd, ratings_schema)
    movies_df = spark.createDataFrame(movies_rdd, movies_schema)

    users_df = users_df.dropna(subset=["user_id", "age", "occupation"])
    ratings_df = ratings_df.dropna(subset=["user_id", "movie_id", "rating"])
    movies_df = movies_df.dropna(subset=["movie_id", "movie_title"])

    return users_df, ratings_df, movies_df


def create_movie_genres_df(movies_df):
    genre_structs = [
        struct(lit(genre).alias("genre"), col(genre).alias("is_genre"))
        for genre in GENRE_COLUMNS
    ]

    return (
        movies_df.select(
            "movie_id",
            "movie_title",
            explode(array(*genre_structs)).alias("genre_info"),
        )
        .select(
            "movie_id",
            "movie_title",
            col("genre_info.genre").alias("genre"),
            col("genre_info.is_genre").alias("is_genre"),
        )
        .filter(col("is_genre") == 1)
        .drop("is_genre")
    )


def write_to_cassandra(df, table_name):
    (
        df.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table=table_name, keyspace=CASSANDRA_KEYSPACE)
        .save()
    )


def read_from_cassandra(spark, table_name):
    return (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table=table_name, keyspace=CASSANDRA_KEYSPACE)
        .load()
    )


def main():
    spark = build_spark_session()

    print("Python and Spark environment")
    print("Spark version: {}".format(spark.version))

    users_df, ratings_df, movies_df = create_dataframes_from_hdfs(spark)
    movie_genres_df = create_movie_genres_df(movies_df)

    users_df.createOrReplaceTempView("users")
    ratings_df.createOrReplaceTempView("ratings")
    movies_df.createOrReplaceTempView("movies")
    movie_genres_df.createOrReplaceTempView("movie_genres")

    # i) Average rating for each movie.
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

    # ii) Top ten movies with the highest average ratings.
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

    # iii) Users who rated at least 50 movies and their favourite genre.
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

    # iv) Users who are less than 20 years old.
    users_under_20_df = spark.sql(
        """
        SELECT user_id, age, gender, occupation, zip_code
        FROM users
        WHERE age < 20
        ORDER BY age ASC, user_id ASC
        """
    )

    # v) Scientists aged between 30 and 40.
    scientists_30_to_40_df = spark.sql(
        """
        SELECT user_id, age, gender, occupation, zip_code
        FROM users
        WHERE occupation = 'scientist'
          AND age BETWEEN 30 AND 40
        ORDER BY age ASC, user_id ASC
        """
    )

    print("\nTask i: Average rating for each movie")
    movie_average_ratings_df.show(10, truncate=False)

    print("\nTask ii: Top ten movies with the highest average ratings")
    top_ten_movies_df.show(10, truncate=False)

    print("\nTask iii: Favourite movie genre for users who rated at least 50 movies")
    favourite_genres_df.show(10, truncate=False)

    print("\nTask iv: Users less than 20 years old")
    users_under_20_df.show(10, truncate=False)

    print("\nTask v: Scientists aged between 30 and 40")
    scientists_30_to_40_df.show(10, truncate=False)

    # Write processed and analytical DataFrames into Cassandra.
    write_to_cassandra(users_df, "users")
    write_to_cassandra(ratings_df, "ratings")
    write_to_cassandra(movies_df.select("movie_id", "movie_title", "release_date", "imdb_url"), "movies")
    write_to_cassandra(movie_average_ratings_df, "movie_average_ratings")
    write_to_cassandra(top_ten_movies_df, "top_ten_movies")
    write_to_cassandra(favourite_genres_df, "favourite_genres")
    write_to_cassandra(users_under_20_df, "users_under_20")
    write_to_cassandra(scientists_30_to_40_df, "scientists_30_to_40")

    # Read Cassandra tables back for validation.
    print("\nValidation: read top_ten_movies from Cassandra")
    cassandra_top_ten_df = read_from_cassandra(spark, "top_ten_movies")
    cassandra_top_ten_df.show(10, truncate=False)

    print("\nValidation: read favourite_genres from Cassandra")
    cassandra_favourite_genres_df = read_from_cassandra(spark, "favourite_genres")
    cassandra_favourite_genres_df.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()