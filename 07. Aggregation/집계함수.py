# Databricks notebook source
##집계 연산
df = spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .load('/databricks-datasets/definitive-guide/data/retail-data/all/*.csv')\
  .coalesce(5)

df.cache()
df.createOrReplaceTempView("dfTable")

df.count()

#count
from pyspark.sql.functions import count
df.select(count("StockCode")).show()

#countDistinct
from pyspark.sql.functions import countDistinct
df.select(count("StockCode")).show()

#approx_count_distinct
from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("StockCode",0.1)).show(2)

#first, last, min, max
from pyspark.sql.functions import first, last,min,max
df.select(first("StockCode").alias("First_stock"),last("StockCode"),min("StockCode"),max("StockCode")).show(2)

#sum,sumDistinct, avg
from pyspark.sql.functions import sum, sumDistinct,avg
df.select(sum("Quantity"),sumDistinct("Quantity"),avg("Quantity")).show(2)

#표본분산 , 표본표준편차
from pyspark.sql.functions import var_samp,stddev_samp
df.select(var_samp("Quantity"),stddev_samp("Quantity")).show(2)

#모분산, 모표본편차
from pyspark.sql.functions import var_pop,stddev_pop
df.select(var_pop("Quantity"),stddev_pop("Quantity")).show(2)

#비대칭도, 척도
from pyspark.sql.functions import skewness, kurtosis
df.select(skewness("Quantity"),kurtosis("Quantity")).show(2)

#공분산과 상관관계
from pyspark.sql.functions import corr,covar_pop,covar_samp
df.select(corr("InvoiceNo","Quantity"),covar_pop("InvoiceNo","Quantity"),covar_samp("InvoiceNo","Quantity")).show(2)

#복합데이터 타입의 집계
from pyspark.sql.functions import collect_set,collect_list
df.agg(collect_set("Country"),collect_list("Country")).show(2)


# COMMAND ----------



# COMMAND ----------


