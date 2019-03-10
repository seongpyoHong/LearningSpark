# Databricks notebook source
from pyspark.sql.functions import expr ,pow , col,round , bround, lit ,corr
#dataframe 생성
df = spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .load("/databricks-datasets/definitive-guide/data/retail-data/by-day/2010-12-01.csv")
df.createOrReplaceGlobalTempView("dfTable")

#pow
fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"),2)+5
df.select(expr("CustomerId"), fabricatedQuantity.alias("RealQuntity")).show(2)
#두개의 column이 모두 수치형 데이터 이므로 계산 가능

#내림
df.select(round(lit("1.6")),bround(lit("1.6"))).show(2)
#두 column의 상관관계
df.stat.corr("Quantity","UnitPrice")
df.select(corr("Quantity","UnitPrice")).show(2)

#describe (평균, 표준편차, 최소,최대,집계)
#통계 스키마는 변경될 수 있으므로 확인용으로만 사용
df.describe().show(6)

#statFunctions package
#stat. 을 통해 접근
ColName = "UnitPrice"
quantiledProbs = [0.5]
relError = 0.05
df.stat.approxQuantile("UnitPrice",quantiledProbs,relError)

#교차표 (Cross-tabulation)
df.stat.crosstab("StockCode","Quantity").show()

#자주 사용하는 항목 확인
df.stat.freqItems(["Quantity"]).show(2)

#모든 ROW에 고유 ID값 추가
from pyspark.sql.functions import monotonically_increasing_id
df.select(monotonically_increasing_id()).show(2)

# COMMAND ----------

1

# COMMAND ----------

1
