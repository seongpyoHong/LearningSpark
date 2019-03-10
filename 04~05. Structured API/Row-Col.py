#DataFrmae 생성 
df = spark.read.format("json").load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")

#스키마 확인
df.printSchema()

#스키마 확인(Data Source)
spark.read.format("json").load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json").schema

#DataFrame에 스키마를 만들고 적용
from pyspark.sql.types import StructField,StructType,StringType,LongType

myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME",StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME",StringType(), True),
  StructField("count",LongType(), False, metadata = {"hello":"world"})
])

df=spark.read.format("json").schema(myManualSchema)\
  .load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")

#컬럼 생성
from pyspark.sql.functions import col, column

col("NewColumn")
column("NewColumn")
#Catalog가 분석을 실행하기 전까지 확인X

#DAG를 나타내는 예제
from pyspark.sql.functions import expr
expr("(((someCol+5)*200)-6)<otherCol")

#DataFrame의 column에 접근
spark.read.format("json").load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json").columns

#Row 객체 확인
df.first()

#Row 객체 생성
from pyspark.sql import Row
myRow = Row("Hello",None,1,False)

#Row 객체 접근
myRow[0]
myRow[2]
