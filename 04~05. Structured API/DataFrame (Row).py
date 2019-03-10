# Databricks notebook source
from pyspark.sql.functions import lit,expr,col
df = spark.read.format("json").load("/databricks-datasets/definitive-guide/data/flight-data/json/2015-summary.json")

#Row 필터링
df.filter(col("count")<2).show(2)
df.where(col("count")<2).show(2)

#여러개의 Filter 적용
df.where(col("count")<2).where(col("DEST_COUNTRY_NAME") != "United States").show(3)

#고유한 Row 얻기
#중복되지 않는 row를 갖는 신규 dataframe 반환
df.select("ORIGIN_COUNTRY_NAME","DEST_COUNTRY_NAME").distinct().count()

#무작위 샘플 만들기
seed =5
#복원 추출 / 비복원추출
withReplacement = False
#추출 비율
fraction = 0.5
df.sample(withReplacement,fraction,seed).count()

#임의 분할 (Random Split)
#머신러닝 알고리즘에서 사용할 set을 만드는데 유용
#총합이 1이 되어야 한다.
RandomDataFrame = df.randomSplit([0.25,0.75],seed)
RandomDataFrame[0].show(2)

#Row 합병 및 추가
#Record를 추가하는 것은 dataframe을 변경하는 작업이기 때문에 불가능
#추가하기 위해서는 새로운 dataframe을 통합해야 한다.

from pyspark.sql import Row
schema = df.schema
newRows = [
  Row("New Country", "Other Country", 5)
]
parallelizedRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizedRows,schema)

#새로운 dataframe 통합
df.union(newDF)\
  .where("count =5")\
  .show(3)

#Row 정렬
df.sort("count").show(4)
df.orderBy(col("count"),col("ORIGIN_COUNTRY_NAME").asc()).show(4)

#로우 수 제한
df.limit(5).show(2)

#최적화 기법 중 하나 : 자주 필터링하는 컬럼을 기준으로 데이터 분할
df.rdd.getNumPartitions()
df.repartition(4)

#특정 컬럼을 기준으로 repartition
df.repartition(5,col("DEST_COUNTRY_NAME"))

#데이터를 셔플없이 병합, coalesce
df.repartition(5,col("DEST_COUNTRY_NAME")).coalesce(2)



# COMMAND ----------



# COMMAND ----------

|
