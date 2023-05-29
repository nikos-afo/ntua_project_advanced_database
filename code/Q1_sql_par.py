from pyspark.sql import SparkSession
import time
start_time = time.time()

spark = SparkSession.builder.appName("advDb_erg").getOrCreate()
movies = spark.read.parquet("hdfs://master:9000/files/movies.parquet")
movies.registerTempTable("movies")

sqlQuery = "Select _c1 as Title, Profit, Year from movies, (Select max(((_c6 - _c5) / _c5) * 100) as Profit,"+ \
           " year(_c3) as Year from movies where _c5 != 0 and _c6 != 0 and year(_c3) >= 2000 "+ \
           "GROUP BY year(_c3)) earning where ((movies._c6 - movies._c5) / movies._c5) * 100 = earning.Profit and year(movies._c3) = earning.Year"
 
res = spark.sql(sqlQuery)

res.write.csv("hdfs://master:9000/outputs/Q1_sql_par_res.csv")
exe_time = time.time() - start_time
file = open("Q1_sql_par.txt", "w")
file.write(str(exe_time) + "\n")
file.close()
