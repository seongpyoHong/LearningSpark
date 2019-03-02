#Static DataSet 분석하여 DataFrame을 생성
StaticDataFrame = spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .load("/databricks-datasets/definitive-guide/data/retail-data/by-day/*.csv")

#View 생성
StaticDataFrame.createOrReplaceTempView("retail_view")
staticSchema = StaticDataFrame.schema

#Window Function : 집계 시에 시계열 column을 기준으로 각 날짜에 대한 전체 데이터를 가지는 윈도우를 구성
from pyspark.sql.functions import window, column, desc, col
StaticDataFrame\
  .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")\
  .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
  .sum("total_cost")\
  .show(5)

#Partition 설정
spark.conf.set("spark.sql.shuffle.partitions","5")

#Streaming Code
streamingDataFrame = spark.readStream\
    .schema(staticSchema)\
    .option("maxFilesPerTrigger", 1)\
    .format("csv")\
    .option("header", "true")\
    .load("/data/retail-data/by-day/*.csv")

#DataFrame이 Streaming 유형인지 확인
streamingDataFrame.isStreaming

#총 판매 금액 계산
purchaseByCustomerPerHour = streamingDataFrame\
  .selectExpr(
    "CustomerId",
    "(UnitPrice * Quantity) as total_cost",
    "InvoiceDate")\
  .groupBy(
    col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
  .sum("total_cost")

#Trigge가 발생한 경우 Inmemoty Table 갱신
#이 경우에서 Trigger -> 이전 집계값보다 큰 값이 발생한 경우 
purchaseByCustomerPerHour.writeStream\
    .format("memory")\#memory = inmemory table에 저장
    .queryName("customer_purchases")\#Inmemory에 저장될 테이블명
    .outputMode("complete")\#Complete = 모든 카운트 수행 결과를 테이블에 저장
    .start()

#Inmemory Table 확인
spark.sql("""
  SELECT *
  FROM customer_purchases
  ORDER BY `sum(total_cost)` DESC
  """)\
  .show(5)
