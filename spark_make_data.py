from pyspark.sql import SparkSession, Window
from pyspark.sql.types import IntegerType, StructType, StringType, FloatType
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

# define csv scheme
time_dim_schema = StructType() \
      .add("year_Q",StringType(),True) \
      .add("year",IntegerType(),True) \
      .add("quarter",IntegerType(),True) \
      .add("start",StringType(),True) \
      .add("end",StringType(),True)         

visits_schema = StructType() \
      .add("customer_id",StringType(),True) \
      .add("visits_date",StringType(),True) \
      .add("test_value_1",FloatType(),True) \
      .add("test_value_2",FloatType(),True) \
      .add("test_value_3",FloatType(),True)
            
time_dim_data = [
      ('2020_Q1',2020,1,'01/01/2020','31/03/2020'),
      ('2020_Q2',2020,2,'01/04/2020','30/06/2020'),
      ('2020_Q3',2020,3,'01/07/2020','30/09/2020'),
      ('2020_Q4',2020,4,'01/10/2020','31/12/2020')] 

visits_data = [
      ('333','22/01/2020',34.0,37.0,66.7),
      ('444','23/01/2020',34.0,37.0,66.7),
      ('555','24/01/2020',34.0,37.0,66.7),
      ('777','25/03/2020',34.0,37.0,66.7),
      ('333','26/03/2020',34.0,37.0,66.7),
      ('444','27/03/2020',34.0,37.0,66.7),
      ('555','28/03/2020',34.0,37.0,66.7),
      ('666','29/03/2020',34.0,37.0,66.7),
      ('666','25/05/2020',34.0,37.0,66.7),
      ('333','26/05/2020',34.0,37.0,66.7),
      ('444','27/05/2020',34.0,37.0,66.7),
      ('555','28/05/2020',34.0,37.0,66.7),
      ('666','29/05/2020',34.0,37.0,66.7),
      ('111','01/10/2020',34.0,37.0,66.7),
      ('111','01/07/2020',34.0,37.0,66.7)
      ]

time_dim_df = spark.createDataFrame(time_dim_data, time_dim_schema)
visits_df = spark.createDataFrame(visits_data, visits_schema)

time_dim_df.write.mode("overwrite").parquet("file:///tmp/time_dim/time_dim_df.parquet")
visits_df.write.mode("overwrite").parquet('file:///tmp/visits/visits_df.parquet')

