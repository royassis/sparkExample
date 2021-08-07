from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructType, StringType, DateType
import pyspark.sql.functions as F
from os.path import abspath
import pyspark
from datetime import datetime

# sc = pyspark.SparkContext('local[*]')
# conf = pyspark.SparkConf()
# conf.setMaster('spark://localhost:7077')
# conf.set('spark.authenticate', False)
# sc = pyspark.SparkContext(conf=conf)
# .config("hive.metastore.uris", "localhost:9083")\


spark = SparkSession \
    .builder \
    .master('spark://localhost:7077')\
    .appName("Python Spark SQL basic example") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# Use this to overwrite specific partition 
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# Read from hive table
spark.sql("use testdb")

# Read from hdfs
# TODO

# define csv scheme
schema = StructType() \
      .add("id",IntegerType(),True) \
      .add("key",IntegerType(),True) \
      .add("val",IntegerType(),True) \


# load csv
df = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("data.csv")
      # .load("hdfs://localhost:9000/tmp/data.csv")

# show data
df.printSchema()
df.show()

# write to parquet
df.repartition(2,"key").write.option("header",True).partitionBy("key").mode('overwrite').parquet(r"data/")

## read parquet
# fs
parquetFile = spark.read.parquet("file:///home/roy/pycharmProject/sparkTest/data")
# hdfs
parquetFile = spark.read.parquet("/tmp/data")

# show data
parquetFile.show()

# agg data
print (parquetFile.agg(F.sum("val")).collect()[0][0])

# Add col
df = parquetFile.withColumn("id_plus_1", F.col("id") + F.lit(1))

# Write to hive table
df.write.partitionBy("key").format("parquet").mode("overwrite").saveAsTable("testdb.test2")
