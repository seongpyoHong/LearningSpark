# Databricks notebook source
##문자열 데이터 타입
from pyspark.sql.functions import initcap,col,lower,upper
#data load
df = spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .load('/databricks-datasets/definitive-guide/data/retail-data/by-day/2010-12-01.csv')
#data schema 확인
df.printSchema()

#initcap : 주어진 문자열에서 공백을 나눠 첫글자를 대문자로 반환
df.select(initcap(col("Description"))).show(2,False)
#lower // upper
df.select(lower(col("StockCode"))).show(2)
#공백 추가 및 제거 (lit,ltrim,rtrim,rpad,lpad,trim)
from pyspark.sql.functions import lit,ltrim,rtrim,rpad,lpad,trim
df.select(
    ltrim(lit("   HELLO   ")).alias("ltrim"),
    rtrim(lit("   HELLO   ")).alias("rtrim"),
    trim(lit("   HELLO   ")).alias("trim"),
    lpad(lit("HELLO"),3," ").alias("lpad"),
    rpad(lit("HELLP"),10," ").alias("rpad")).show(2)


##정규 표현식
#description컬럼의 값을 COLOR 값으로 치환
from pyspark.sql.functions import regexp_replace
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
  regexp_replace(col("Description"),regex_string,"COLOR").alias("color_clean"),col("Description")
).show(2)

#주어진 문자를 다른 문자로 치환
from pyspark.sql.functions import translate
df.select(translate(col("Description"),"WHI","123")).show(2)

#color name 추출
from pyspark.sql.functions import regexp_extract

extract_str="(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(
  regexp_extract(col("Description"),extract_str,1).alias("color_clean")
).show(6)

#data의 존재여부 확인
#instr
from pyspark.sql.functions import instr
containBlack=instr(col("Description"),"BLACK")>=1
df.withColumn("HasSimpleColor",containBlack)\
  .where("HasSimpleColor")\
  .select("Description").show(15,False)

#인수의 개수가 동적으로 변할 때
from pyspark.sql.functions import expr, locate
simpleColors = ["black", "white", "red", "green", "blue"]
def color_locator(column, color_string):
  return locate(color_string.upper(), column)\
          .cast("boolean")\
          .alias("is_" + color_string)
selectedColumns = [color_locator(df.Description, c) for c in simpleColors]
selectedColumns.append(expr("*")) # has to a be Column type

df.select(*selectedColumns).where(expr("is_white OR is_red"))\
  .select("Description").show(3, False)




