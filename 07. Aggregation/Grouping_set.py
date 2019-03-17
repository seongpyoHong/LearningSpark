
#그룹화 셋
dfNoNull = newDF.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")

#SQL 구문
#SELECT CustomerId, stockCode, sum(Quantity) FROM dfNoNull
#GROUP BY customerId, stockCode GROUPING SETS((customerId, stockCode))
#ORDER BY customerId DESC, stockCode DESC

#rollup / cube를 이용하여 DataFrame에서도 사용 가능
#rollup
from pyspark.sql.functions import sum
rollupDF = dfNoNull.rollup("Date","Country").agg(sum("Quantity"))\
  .selectExpr("Date","Country")\
  .orderBy("Date")
rollupDF.show()

#Cube
cubeDF = dfNoNull.cube("Date","Country").agg(sum("Quantity"))\
  .selectExpr("Date","Country")\
  .orderBy("Date")
cubeDF.show()

#groupint Metadata
#groupint_id : 결과 데이터 셋의 집계수준을 명시하는 컬럼 제공
#scala에서 사용 가능(반환값이 Dataset이기 때문에)

#pivot : row -> column
pivoted = newDF.groupBy("date").pivot("Country").sum()

#사용자 정의 집계함수
#scala와 java에서만 사용 가능

