#스파크는 실행계획 수립과 처리에 사용하는 자체 데이터 타입 정보를 가지고 있는 Catalyst엔진을 사용한다. => 여러 언어 API와 직접 매핑된다.
df = spark.range(500).toDF("number")
df.select(df["number"] + 10)
df.count()
# 스파크에서 덧셈 연산 수행

#range method를 사용해 DataFrame 생성
spark.range(2).collect()
#Row객체로 이루어진 배열 반환

#스파크 데이터 타입 import
from pyspark.sql.types import *
b=ByteType()

