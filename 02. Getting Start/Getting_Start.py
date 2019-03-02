#SparkSession(드라이브 프로세스) 확인
spark

#일정 범위의 숫자 생성 -> 각 숫자가 서로 다른 executor에 할당
myRange = spark.range(1000).toDF("number")

#transformation 
#짝수를 찾는 예제
#action을 호출하지 않으면 결과가 출력되지 않는다.
divisBy2 = myRange.where("number %2 =0")

#Action : 실제 연산 수행
#count() -> dataframe의 전체 레코드 수를 반환
divisBy2.count()

