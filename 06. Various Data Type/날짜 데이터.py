##날짜 데이터
df.printSchema()
from pyspark.sql.functions import current_date,current_timestamp
dateDF = spark.range(10)\
  .withColumn("today",current_date())\
  .withColumn("now",current_timestamp())

dateDF.printSchema()

#5일 전후의 날짜 계산
from pyspark.sql.functions import date_add , date_sub
dateDF.select(date_add(col("today"),5),date_sub(col("today"),5)).show(3)

#두 날짜의 차이 계산
from pyspark.sql.functions import datediff,months_between,to_date

dateDF.withColumn("week_ago",date_sub(col("today"),7))\
  .select(datediff(col("week_ago"),col("today"))).show(2)

dateDF.select(to_date(lit("2015-03-01")).alias("start"),
              to_date(lit("2019-03-14")).alias("end"))\
      .select(months_between(col("start"),col("end"))).show(2)

#to_date : string -> date
spark.range(5).withColumn("date",lit("2017-01-01"))\
  .select(to_date(col("date"))).show(2)

#값이 없을 경우 null값을 반환
dateDF.select(to_date(lit("2016-20-11"))).show(2)

#날짜 format지정
dateFormat = "yyyy-dd-MM"
cleanDataDF = spark.range(3).select(
  to_date(lit("2017-20-11"),dateFormat).alias("date"),
  to_date(lit("2017-31-12"),dateFormat).alias("date2")).show(2)

#to_timestamp
from pyspark.sql.functions import to_timestamp
cleanDataDF.select(to_timestamp(col("date"),dateFormat)).show()
