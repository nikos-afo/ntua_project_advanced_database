from pyspark.sql import SparkSession
import time

start_time = time.time()
spark = SparkSession.builder.appName("advDb_erg").getOrCreate()

movie_genres = spark.read.parquet("hdfs://master:9000/files/movie_genres.parquet")
ratings = spark.read.parquet("hdfs://master:9000/files/ratings.parquet")

ratings.registerTempTable("ratings")
movie_genres.registerTempTable("movie_genres")

sqlQuery = "SELECT  movie_genres._c1, count(distinct movie_genres._c0) as total_per_category, avg(avg_ratings.average) "+ \
           " FROM movie_genres INNER JOIN"+ \
           "(select _c1 as movie_id, avg(_c2) as average from ratings group by _c1) "+ \
           "avg_ratings ON movie_genres._c0=avg_ratings.movie_id group by movie_genres._c1"
res= spark.sql(sqlQuery)

res.write.csv("hdfs://master:9000/outputs/Q3_sql_par_res.csv")
exe_time = time.time() - start_time
file = open("Q3_sql_par.txt", "w")
file.write(str(exe_time) + "\n")
file.close()
