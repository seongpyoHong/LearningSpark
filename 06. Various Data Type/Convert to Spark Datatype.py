# Databricks notebook source
#dataframe 생성
df = spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .load("/databricks-datasets/definitive-guide/data/retail-data/by-day/2010-12-01.csv")

df.printSchema()
df.createOrReplaceGlobalTempView("dfTable")

#스파크 타입으로 변경
#lit : 다른 언어의 data type을 스파크에 맞게 변환
from pyspark.sql.functions import lit
df.select(lit(5),lit("five"),lit(5.0))

#Boolean Type
from pyspark.sql.functions import col

df.where(col("InvoiceNo") != 536365)\
  .select("InvoiceNo")\
  .show(5,False)

#위의 코드 간략히
df.where("InvoiceNo > 536356")\
  .show(2,False)

# COMMAND ----------

1

# COMMAND ----------

1
