# Databricks notebook source
##Handling NULL data
from pyspark.sql.functions import initcap,col,lower,upper
#data load
df = spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .load('/databricks-datasets/definitive-guide/data/retail-data/by-day/2010-12-01.csv')
#data schema 확인
df.printSchema()
#coalesce : null이 아닌 첫번째 값을 반환
from pyspark.sql.functions import coalesce
df.select(coalesce(col("Description"),col("CustomerID"))).show(3)
#drop : 로우 제거
df.na.drop('all')
df.na.drop('any')
df.na.drop("all",subset=["StockCode","InvoiceNo"])

#fill : column을 특정 값으로 채운다.
df.na.fill("all",subset=["StockCode","InvoiceNo"])

#replace 
df.na.replace([""],["UNKNOWN"],"Description")

