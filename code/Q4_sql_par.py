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
 
sqlQuery = "select "+ \
           "case when RIGHT(YEAR(MOVIES._C3),2) between 00 and 04 then '1i pentaetia'"+ \
           "when RIGHT(YEAR(MOVIES._C3),2) between 05 and 09 then '2i pentaetia'"+ \
           " when RIGHT(YEAR(MOVIES._C3),2) between 10 and 14 then '3i pentaetia' "+ \
           "when RIGHT(YEAR(MOVIES._C3),2) between 15 and 19 then '4i pentaetia' end AS PENTAETIA"+ \
           ",avg(character_length(movies._c2)-character_length(replace(movies._c2, ' ', ''))+1)as meso_mikos "+ \
           "from movies"+ \
           "inner join movie_genres on movies._c0=movie_genres._c0 "+ \
           "where movie_genres._c1='Drama' "+ \
           "and movies._c2 is not null "+ \
           "and year(movies._c3)>=2000 group by pentaetia"

res= spark.sql(sqlQuery)

res.write.csv("hdfs://master:9000/outputs/Q4_sql_par_res.csv")
exe_time = time.time() - start_time
file = open("Q4_sql_par.txt", "w")
file.write(str(exe_time) + "\n")
file.close()
