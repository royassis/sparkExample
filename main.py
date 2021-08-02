from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructType
import pyspark.sql.functions as F

# sc = pyspark.SparkContext('local[*]')

# define csv scheme
schema = StructType() \
      .add("id",IntegerType(),True) \
      .add("key",IntegerType(),True) \
      .add("val",IntegerType(),True) \

# create SparkSession
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Use this to overwrite specific partition 
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# load csv
df = spark.read.format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("data.csv")

# show data
df.printSchema()
df.show()

# write to parquet
df.repartition(2).write.option("header",True).partitionBy("key").mode('overwrite').parquet(r"data/")

# read parquet
parquetFile = spark.read.parquet("data")

# show data
parquetFile.show()

# agg data
print (parquetFile.agg(F.sum("val")).collect()[0][0])


