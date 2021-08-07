# Read from hdfs

# Spark
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("myFirstApp").setMaster("local")
sc = SparkContext(conf=conf)

textFile = sc.textFile("hdfs://localhost:9000/tmp/data.csv")
print(textFile.first())


# pydoop
import pydoop.hdfs as hd
import pandas as pd
with hd.open("/tmp/data.csv") as f:
    df =  pd.read_csv(f)
print(df)