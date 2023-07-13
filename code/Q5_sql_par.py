
from pyspark.sql import SparkSession
import time
start_time = time.time()

spark = SparkSession.builder.appName("advDb_erg").getOrCreate()

movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
movie_genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")

ratings.registerTempTable("ratings")
movie_genres.registerTempTable("movie_genres")
movies.registerTempTable("movies")

sqlQuery = "WITH user_genre as "+ \
           "(select max.genre, vote_counts.user_id, max.max_votes from"+ \
           "(select movie_genres._c1 as genre, ratings._c0 as user_id, count(*) as votes from "+ \
           "movie_genres inner join ratings on ratings._c1 = movie_genres._c0 "+ \
           "group by movie_genres._c1, ratings._c0) vote_counts, "+ \
           "(select genre, max(votes) as max_votes from "+ \
           "(select movie_genres._c1 as genre, ratings._c0 as user_id, count(*) as votes from "+ \
           "movie_genres inner join ratings on ratings._c1 = movie_genres._c0 "+ \
           "group by movie_genres._c1, ratings._c0) vote_counts "+ \
           "group by genre) max "+ \
           "where max.genre = vote_counts.genre and max.max_votes = vote_counts.votes), "+ \
           "popular as "+ \
           "(select _c1 as movie_id, count(*) as popularity from "+ \
           "ratings group by _c1), "+ \
           "max_movies as"+ \
           "(select ratings._c0 as user_id, movies._c0 as max_movie_id, movies._c1 as max_movie, ratings._c2 as max_rating, movie_genres._c1 as genre from "+ \
           "(movies inner join movie_genres on movies._c0 = movie_genres._c0) inner join ratings on movies._c0 = ratings._c1 "+ \
           "where (ratings._c0, ratings._c2, movie_genres._c1) in (select ratings._c0, max(ratings._c2), movie_genres._c1 from ratings inner join movie_genres on movie_genres._c0 = ratings._c1 group by ratings._c0, movie_genres._c1)), "+ \
           "min_movies as "+ \
           "(select ratings._c0 as user_id, movies._c0 as min_movie_id, movies._c1 as min_movie, ratings._c2 as min_rating, movie_genres._c1 as genre from "+ \
           "(movies inner join movie_genres on movies._c0 = movie_genres._c0) inner join ratings on movies._c0 = ratings._c1 "+ \
           "where (ratings._c0, ratings._c2, movie_genres._c1) in (select ratings._c0, min(ratings._c2), movie_genres._c1 from ratings inner join movie_genres on movie_genres._c0 = ratings._c1 group by ratings._c0, movie_genres._c1)) "+ \
           "select max_genre.genre, max_genre.user_id, max_genre.max_votes, max_genre.max_movie, max_genre.max_rating, min_pop.min_movie, min_pop.min_rating from "+ \
           "(select user_genre.genre, user_genre.user_id, user_genre.max_votes, max_pop.max_movie, max_pop.max_rating from user_genre inner join "+ \
           "(select temp1.user_id, temp1.genre, temp1.max_rating, temp1.max_movie from (select * from max_movies inner join popular on max_movies.max_movie_id = popular.movie_id) temp1, "+ \
           "(select max_movies.user_id, max_movies.genre, max(popularity) as popularity from "+ \
           "max_movies inner join popular on max_movies.max_movie_id = popular.movie_id group by max_movies.user_id, max_movies.genre) temp2 "+ \
           "where temp1.user_id = temp2.user_id and temp1.genre = temp2.genre and temp1.popularity = temp2.popularity) max_pop "+ \
           "on user_genre.user_id = max_pop.user_id and user_genre.genre = max_pop.genre) max_genre inner join "+ \
           "(select temp1.user_id, temp1.genre, temp1.min_rating, temp1.min_movie from (select * from min_movies inner join popular on min_movies.min_movie_id = popular.movie_id) temp1, "+ \
           "(select min_movies.user_id, min_movies.genre, max(popularity) as popularity from "+ \
           "min_movies inner join popular on min_movies.min_movie_id = popular.movie_id group by min_movies.user_id, min_movies.genre) temp2 "+ \
           "where temp1.user_id = temp2.user_id and temp1.genre = temp2.genre and temp1.popularity = temp2.popularity) min_pop "+ \
           "on max_genre.user_id = min_pop.user_id and max_genre.genre = min_pop.genre order by max_genre.genre" 
            
          
res= spark.sql(sqlQuery)

res.write.csv("hdfs://master:9000/outputs/Q5_sql_par_res.csv")
exe_time = time.time() - start_time
file = open("Q5_sql_par.txt", "w")
file.write(str(exe_time) + "\n")
file.close()
