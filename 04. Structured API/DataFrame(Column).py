# Databricks notebook source
from pyspark.sql.functions import lit,expr,col
df = spark.read.format("json").load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")

#Column 생성
df.withColumn("numberOne", lit(1)).show(2)

#표현식 생성
df.withColumn("withinCountry",expr("DEST_COUNTRY_NAME== ORIGIN_COUNTRY_NAME")).show(2)

#withinColumn으로 Column name 변경
df.withColumn("Dsetination",expr("DEST_COUNTRY_NAME")).show(2)

#Column 변경
df.withColumnRenamed("DEST_COUNTRY_NAME","dest").columns

#예약 문자와 키워드
#Escape 문자가 필요 없는 경우 
#withColumn 메서드의 첫 번쨰 인수로 새로운 컬럼명을 나타내는 문자열을 지정했기 때문에
dfWithLongCol = df.withColumn(
   "This Long Column-Name",
  expr("ORIGIN_COUNTRY_NAME")
)

#Escape 문자가 필요한 경우
#표현식으로 Column을 참조하므로 (') 필요

dfWithLongCol.selectExpr(
  "`This Long Column-Name`",
  "`This Long Column-Name` as `new col`").show(2)

#문자열을 사용해 컬럼을 참조하면 리터럴로 해석
#표현식에만 이스케이프 처리가 필요하다.

#컬럼 제거 (다수 가능)
df.drop("ORIGIN_COUNTRY_NAME").columns

#컬럼의 데이터 타입 변경
df.withColumn("count2",col("count").cast("string"))


# COMMAND ----------



# COMMAND ----------

|
