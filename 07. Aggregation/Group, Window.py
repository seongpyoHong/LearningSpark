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

#그룹의 기준이 되는 컬럼으 여러 개 지정
df.groupBy("InvoiceNo","CustomerId").count().show()

#표현식을 이용한 그룹화 (agg method)
from pyspark.sql.functions import count,expr,avg,stddev_pop
df.groupBy("InvoiceNo").agg(
  count("Quantity").alias("Count_Quan"),
  expr("count(Quantity)")
).show()

#map을 이용한 그룹화
df.groupby("InvoiceNo").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)").alias("stddev")).show()

#window function
from pyspark.sql.functions import col,to_date,max,dense_rank,rank

#새로운 DataFrame 생성
newDF=df.withColumn("date",to_date(col("InvoiceDate"),"MM/d/yyyy H:mm"))

from pyspark.sql.window import Window
from pyspark.sql.functions import desc

#윈도우 명세 작성
windowSpec = Window\
  .partitionBy("CustomerId","date")\
  .orderBy(desc("Quantity"))\
  .rowsBetween(Window.unboundedPreceding,Window.currentRow)

#집계 함수에 컬럼명이나 표현식을 전달 / 이 함수를 적용할 DF이 정의된 윈도우 명세도 함께 사용
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)
# => 컬럼이나 표현식 반환하므로 select 구문 사용 가능

#최대 구매수량 날짜 확인
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)

#select를 통해 반환값 확인
newDF.where("CustomerId IS NOT NULL").orderBy("CustomerId")\
  .select(
    col("CustomerId"),col("date"),col("Quantity"),
    purchaseRank.alias("Rank"),purchaseDenseRank.alias("Dense_Rank"),maxPurchaseQuantity.alias("Max")
).show()



