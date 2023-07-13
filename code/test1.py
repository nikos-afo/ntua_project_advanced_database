from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Q_rdd").getOrCreate()

from io import StringIO
import csv
def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

sc = spark.sparkContext

res = sc.textFile("hdfs://master:9000/outputs/Q5_sql_par_res.csv")


for i in res.collect():
    print(i)
