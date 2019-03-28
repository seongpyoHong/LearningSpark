# Databricks notebook source
##Broadcast 변수

myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"\
  .split(" ")
words = spark.sparkContext.parallelize(myCollection,2)

#Broadcast 변수 선언
supplementalData = {"Spark" : 1000 , "Definitive" : 200,
                    "Big" : 300, "Simple" : 100}
suppBroadcast = spark.sparkContext.broadcast(supplementalData)

suppBroadcast.value

#Broadcast 변수를 이용하여 RDD 변환
words.map(lambda word : (word,suppBroadcast.value.get(word,0)))\
  .sortBy(lambda wordPair : wordPair[1])\
  .collect()

##브로드 캐스트 변수를 사용하는 것이 클로져에 담아 전달하는 것보다 훨씬 효율적
#Accumulator
flights = spark.read\
  .parquet("/databricks-datasets/definitive-guide/data/flight-data/parquet/2010-summary.parquet")

#이름이 지정되지 않은 어큐물레이터 생성
accChina = spark.sparkContext.accumulator(0)

#어큐물레이터에 값을 더하는 방법 정의
def accChinaFunc(flight_row):
  destination = flight_row["DEST_COUNTRY_NAME"]
  origin = flight-row["ORIGIN_COUNTRY_NAME"]
  
  if destination == "China":
    accChina.add(flight_row["count"])
  if origin =="China":
    accChina.add(flight_row["count"])
    
flights.foreach(lambda flight_row : accChinaFunc(flight_row))


# COMMAND ----------



# COMMAND ----------


