# Databricks notebook source
###데이터 소스
## CSV
#파일 읽기
csvFile = spark.read.format("csv")\
  .option("header","true")\
  .option("mode","FAILFAST")\
  .option("inferSchema","true")\
  .load("/databricks-datasets/definitive-guide/data/flight-data/csv/2010-summary.csv")

#파일 쓰기
csvFile.write.format("csv").mode("overwrite").option("sep","\t")\
  .save("/tmp/my-tsv-file.tsv")

##JSON
#파일 읽기
#여러 줄로 구성된 방식을 사용하기 위해서는 MultiLine 옵션 사용
spark.read.format("json").option("mode","FAILFAST").option("inferSchema","true")\
  .load("/databricks-datasets/definitive-guide/data/flight-data/json/2010-summary.json").show()

#파일 쓰기
csvFile.write.format("json").mode("overwrite").save("/tmp/my-json-file.json")

##Parquet
#파일 읽기
spark.read.format("parquet").load("/databricks-datasets/definitive-guide/data/flight-data/parquet/2010-summary.parquet").show()
#파일 쓰기
csvFile.write.format("parquet").mode("overwrite").save("/tmp/my-parquet-file.parquet")

##ORC
#파일 읽기
spark.read.format("orc").load("/databricks-datasets/definitive-guide/data/flight-data/orc/2010-summary.orc").show()
#파일 쓰기
csvFile.write.format("orc").mode("overwrite").save("\tmp\my-orc-file.orc")

