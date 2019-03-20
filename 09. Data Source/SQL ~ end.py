# Databricks notebook source
###데이터 소스
##SQL
#접속 관련 속성 정의
driver = "org.sqlite.JDBC"
path = "/databricks-datasets/definitive-guide/data/data/flight-data/jdbc/my-sqlite.db"
url = "jdbc:sqlite:" + path
tablename = "flight_info"

#SQL 데이터 -> DataFrame
dbDataFrame = spark.read.format("jdbc").option("url",url)\
  .option("dbtable",tablename).option("driver",driver).load()

#query pushdown
dbDataFrame.filter("DEST_COUNTRY_NAME in ('Auguilla','Sweden')").explain()

#SQL Query 명시
pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info AS flight_info"""
dbDataFrame = spark.read.format("jdbc")\
  .option("url",url).option("dbtable",pushdownQuery).option("driver",driver)\
  .load()

#DB 병렬로 읽기
dbDataFrame = spark.read.format("JDBC")\
  .option("url",url).option("dbtable",dbtable).option("driver",driver)\
  .option("numPartitions",10).load()

#조건절 목록을 정의해 스파크 자체 파티션에 결과 데이터 저장 -> 더 많은 처리 가능 
props = {"driver":"org.sqlite.JDBC"}
predicates = [
  "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
  "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'"]
spark.read.jdbc(url, tablename, predicates=predicates, properties=props).show()
spark.read.jdbc(url,tablename,predicates=predicates,properties=props)\
  .rdd.getNumPartitions() # 2

#조건절을 기반으로 분할
colName = "count"
lowerBound = 0L
upperBound = 348113L
numPartitions =10

spark.read.jdbc(url, tablename, column=colName, properties=props,
                lowerBound=lowerBound, upperBound=upperBound,
                numPartitions=numPartitions).count() # 255
#sql db 쓰기
newPath = "jdbc:sqlite://tmp/my-sqlite.db"
csvFile.write.jdbc(newPath, tablename, mode="overwrite", properties=props)



# COMMAND ----------



# COMMAND ----------


