from pyspark.sql import SparkSession
import time

start_time = time.time()
spark = SparkSession.builder.appName("Q_rdd").getOrCreate()
sc = spark.sparkContext
res = sc.textFile("hdfs://master:9000/files/movie_genres.csv"). \
         take(100)

sc.parallelize(res).saveAsTextFile("hdfs://master:9000/files/movie_genres_100.csv")
