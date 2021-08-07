from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

time_dim_df = spark.read.parquet("file:///tmp/time_dim/*.parquet")
visits_df = spark.read.parquet("file:///tmp/visits/*.parquet")

time_dim_df = time_dim_df.withColumn('start',F.to_timestamp(time_dim_df.start, 'dd/MM/yyyy'))
time_dim_df = time_dim_df.withColumn('end',F.to_timestamp(time_dim_df.end, 'dd/MM/yyyy'))
visits_df = visits_df.withColumn('visits_date',F.to_timestamp(visits_df.visits_date, 'dd/MM/yyyy'))

merge=  visits_df.join(time_dim_df,
      (visits_df.visits_date >= time_dim_df.start)
      & (visits_df.visits_date <= time_dim_df.end) 
  )

agg_list = [F.min('visits_date').alias('min_date'),
            F.max('visits_date').alias('max_date')]
for i in range(1,4):
      agg_list.append(F.mean(f'test_value_{i}').alias(f'mean{i}'))
      agg_list.append(F.sum(f'test_value_{i}').alias(f'sum{i}'))
            
groups = ["customer_id","year","quarter"]
merged = merge.groupBy(groups).agg(*agg_list)

windowval = (Window.partitionBy("customer_id").orderBy(["year","quarter"])
             .rangeBetween(Window.unboundedPreceding, 0))

merged= merged.withColumn('sum1', F.sum('sum1').over(windowval))
merged= merged.withColumn('sum2', F.sum('sum2').over(windowval))
merged= merged.withColumn('sum3', F.sum('sum3').over(windowval))

merged = merged.withColumn('daydiff', F.datediff(merged.max_date, merged.min_date))

merged = merged.withColumn('next_quarter' ,(merged.quarter % 4) + 1 )
merged= merged.withColumn('next_year',F.when(merged.quarter > merged.next_quarter, merged.year + F.lit(1))\
      .otherwise(merged.year))

right = merged.select(["customer_id","next_quarter","next_year",'sum1'])\
      .withColumnRenamed('sum1','prev_sum1')

merged = merged.drop('next_year', 'next_quarter')
merged = merged.join(right.alias("r"),
            (merged.year == right.next_year) & 
            (merged.quarter == right.next_quarter) & 
            (merged.customer_id == right.customer_id),
            "left")\
         .drop(right.customer_id)
      
merged = merged.withColumn("sum1_prec", F.abs(merged.sum1 - merged.prev_sum1)/merged.sum1)

columns = ['customer_id',
          'year',
          'quarter',
          'next_quarter',
          'next_year',
          'mean1',
          'mean2',
          'mean3',
          'sum1',
          'sum2',
          'sum3',
          'daydiff',
          'sum1_prec']
final = merged.sort(*groups).select(columns)

p = "file:///tmp/results"
final.write.partitionBy(["year","quarter"]).mode("overwrite").parquet(p)