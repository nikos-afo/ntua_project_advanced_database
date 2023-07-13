from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("advDb_erg").getOrCreate()

ratings= spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/ratings.csv")
movie_genres = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movie_genres.csv")
movies = spark.read.format("csv").options(header='false', inferSchema='true').load("hdfs://master:9000/files/movies.csv")

def group_by_5years(x):
    if x >= 2000 and x < 2005:
        return 1
    if x >= 2005 and x < 2010:
        return 2
    if x >= 2010 and x < 2015:
        return 3
    if x >= 2015 and x < 2019:
        return 4
    return -1

def sum_len(x):
    return len(x.split(" "))


ratings.registerTempTable("ratings")
movie_genres.registerTempTable("movie_genres")
movies.registerTempTable("movies")

spark.udf.register("grouper", group_by_5years)
spark.udf.register("sum_len", sum_len)

sqlQuery = "select group, avg(sum_len) from "+ \
           "(select grouper(year(movies._c3)) as group, sum_len(movies._c2) as sum_len "+ \
           "from movies INNER JOIN movie_genres on movies._c0 = movie_genres._c0 "+ \
           "where movie_genres._c1 = 'Drama' and movies._c2 is not NULL and year(movies._c3) >= 2000) summary_len "+ \
           "GROUP BY group"

res= spark.sql(sqlQuery)

res.show()
