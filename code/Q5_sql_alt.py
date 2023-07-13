
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("advDb_erg").getOrCreate()

ratings= spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/ratings.csv")
movie_genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movie_genres.csv")
movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movies.csv")

ratings.registerTempTable("ratings")
movie_genres.registerTempTable("movie_genres")
movies.registerTempTable("movies")


sqlQuery = "select * from (select movie_genres._c1, ratings._c0, count(*) as cnt"+ \
           "from movie_genres._c1 "+ \
           "inner join ratings on ratings._c1=movie_genres._c0 "+ \
           "group by movie_genres._c1,ratings._c0)a "+ \
           "inner join ( select movie_genres._c1, max(cnt) as max_cnt"+ \
           "from (select movie_genres._c1, a.ratings._c0,count(*) as cnt "+ \
           "from movie_genres._c1 inner join ratings on ratings._c1=movie_genres._c0  "+ \
           "group by movie_genres._c1,ratings._c0) group by movie_genres._c1)b on a.movie_genres._c1=b.movie_genres._c1 "+ \
           "where a.cnt = b.max_cnt"
 
 
res= spark.sql(sqlQuery)

res.show()

