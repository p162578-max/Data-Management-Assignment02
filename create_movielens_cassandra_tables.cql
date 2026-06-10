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
