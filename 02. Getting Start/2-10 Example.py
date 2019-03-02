#2-10 Example
flightData2015 = spark\
  .read\
  .option("inferSchema", "true")\
  .option("header", "true")\
  .csv("/databricks-datasets/definitive-guide/data/flight-data/csv/2015-summary.csv")

#confirm the three datas from top.
flightData2015.take(3)
#sorting to column(integer)
#confirm the explain plan
flightData2015.sort("count").explain()

#Set ouput shuffle partition to 5(dafault : 200)
spark.conf.set("spark.sql.shuffle.partitions","5")

#check sorting
flightData2015.sort("count").take(2)

#DataFrame -> table or view
flightData2015.createOrReplaceTempView("flight_data_2015")

# SQL 구문
sqlWqy = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_Data_2015
GROUP BY DEST_COUNTRY_NAME
""")

# dataframe 구문
dataFrameWay = flightData2015\
  .groupBy("DEST_COUNTRY_NAME")\
  .count()

#Compare with explain plan
sqlWqy.explain()
dataFrameWay.explain()

#Use max function
from pyspark.sql.functions import max
flightData2015.select(max("count")).take(1)

#Top 5 of country which have destination_total (SQL)
maxSql = spark.sql("""
SELECT DEST_COUNTRY_NAME, sum(count) as destination_total
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
ORDER BY sum(count) DESC
LIMIT 5
""")
maxSql.show()

#Top 5 of country which have destination_total (DataFrame)
from pyspark.sql.functions import desc

flightData2015\
  .groupby("DEST_COUNTRY_NAME")\
  .sum("count")\
  .withColumnRenamed("sum(count)","destination_total")\
  .sort(desc("destination_total"))\
  .limit(5)\
  .show()
