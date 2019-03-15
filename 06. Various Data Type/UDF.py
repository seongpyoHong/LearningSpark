# Databricks notebook source
##UDF
#실제 함수 생성
udfExampleDF = spark.range(5).toDF("num")
def power3(double_value):
  return double_value **3
power3(2.0)

#함수 등록(DataFrame에 등록)
from pyspark.sql.functions import udf
power3udf = udf(power3)

#함수 사용
from pyspark.sql.functions import col
udfExampleDF.select(power3(col("num"))).show(3)

#함수 등록(sparkSQL에 등록)
from pyspark.sql.types import IntegerType, DoubleType
spark.udf.register("power3py",power3,IntegerType())
udfExampleDF.selectExpr("power3py(num)").show(2)
