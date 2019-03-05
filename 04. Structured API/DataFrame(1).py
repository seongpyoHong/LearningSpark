#DataFrame 생성
df = spark.read.format("json").load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")

#SQL의 쿼리를 실행하기 위한 View생성
df.createOrReplaceGlobalTempView("dfTable")

#Row 객체를 가진 Seq타입을 직접 변환
from pyspark.sql import Row
from pyspark.sql.types import StructField,StructType,StringType,LongType

myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME",StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME",StringType(), True),
  StructField("count",LongType(), False)
])

myRow = Row("Hello",None,1)
myDf =spark.createDataFrame([myRow],myManualSchema)
myDf.show()

#select
df.select("DEST_COUNTRY_NAME").show(2)
df.select("DEST_COUNTRY_NAME","count").show(3)

#Column 참조
from pyspark.sql.functions import expr,col,column

df.select(
  expr("DEST_COUNTRY_NAME"),
  col("DEST_COUNTRY_NAME"),
  column("DEST_COUNTRY_NAME"))\
  .show(3)

#컬럼명 변경
df.select(expr("DEST_COUNTRY_NAME AS destination")).show(3)

#위의 표현식의 결과를 다시 변경
df.select(expr("DEST_COUNTRY_NAME AS destination").alias("DEST_COUNTRY_NAME"))\
  .show(2)

#selectExpr
df.selectExpr("DEST_COUNTRY_NAME as newColumnName" , "DEST_COUNTRY_NAME").show(3)

#출발지와 도착지가 같은지 나타내는 새로운 column 추가
df.selectExpr(
"*",
"(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) AS withinCountry")\
.show(3)

#column에 대한 집계 함수 지정
df.selectExpr("avg(count)","count(distinct(ORIGIN_COUNTRY_NAME))").show(2)

#스파크 데이터 타입으로 변환
from pyspark.sql.functions import lit
df.select(expr("*"),lit(1).alias("ONE")).show(2)
