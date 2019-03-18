# Databricks notebook source
#데이터 셋 생성
person = spark.createDataFrame([
    (0, "Bill Chambers", 0, [100]),
    (1, "Matei Zaharia", 1, [500, 250, 100]),
    (2, "Michael Armbrust", 1, [250, 100])])\
  .toDF("id", "name", "graduate_program", "spark_status")
graduateProgram = spark.createDataFrame([
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley")])\
  .toDF("id", "degree", "department", "school")
sparkStatus = spark.createDataFrame([
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor")])\
  .toDF("id", "status")

#내부 조인
joinExpression = person["graduate_program"]==graduateProgram['id']
#모두 키가 존재하지 않을 경우 비어있는 df 반환
wrongJoinExpression =person["name"]==graduateProgram["school"]
#조인 타입 지정 => 내부조인
joinType ="inner"
person.join(graduateProgram,joinExpression,joinType).show(2)

#외부조인
joinType="outer"
person.join(graduateProgram,joinExpression,joinType).show()

#왼쪽 외부 조인
jointype = "left_outer"
graduateProgram.join(person,joinExpression,joinType).show()

#오른쪽 외부 조인
jointype = "right_outer"
person.join(graduateProgram,joinExpression,joinType).show()

# 왼쪽 세미 조인
joinType="left_semi"
graduateProgram.join(person,joinExpression,joinType).show()

#왼쪽 안티 조인
joinType="left_anti"
graduateProgram.join(person,joinExpression,joinType).show()

#교차조인
person.crossJoin(graduateProgram).show()

##복합 데이터 타입의 조인
from pyspark.sql.functions import expr,col

person.withColumnRenamed("id","personId")\
  .join(sparkStatus,expr("array_contains(spark_status,id)")).show()



# COMMAND ----------



# COMMAND ----------


