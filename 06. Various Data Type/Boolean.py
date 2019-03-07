# Databricks notebook source
from pyspark.sql.functions import instr
#dataframe 생성
df = spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .load("/databricks-datasets/definitive-guide/data/retail-data/by-day/2010-12-01.csv")

df.printSchema()
df.createOrReplaceGlobalTempView("dfTable")

#and를 차례대로 배치 / or는 한 조건에 같이
priceFilter = col("UnitPrice")> 600
descripFilter = instr(df.Description, "POSTAGE") >=1
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show(3)

#Boolean Column 사용 dataframe 필터링
DotCodeFilter = col("StockCode") == "DOT"
df.withColumn("isExpensive",DotCodeFilter &(priceFilter | descripFilter))\
  .where("isExpensive")\
  .select("unitPrice","isExpensive").show(5)

#filter 사용 X
df.withColumn("isExpensive",expr("NOT UnitPrice <= 250"))\
  .where("isExpensive")\
  .select("Description","UnitPrice").show(5)

# COMMAND ----------

1

# COMMAND ----------

1
