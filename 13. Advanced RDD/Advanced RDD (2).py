# Databricks notebook source
myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"\
  .split(" ")
words = spark.sparkContext.parallelize(myCollection,2)

# key-value 형태의 RDD
words.map(lambda word:(word.lower(),1))
#keyBy : key 생성
keyword = words.keyBy(lambda word : word.lower()[0])
#mapping value
keyword.mapValues(lambda word:word.upper()).collect()
keyword.flatMapValues(lambda word:word.upper()).collect()
#key 추출
keyword.keys().collect()
#value 추출
keyword.values().collect()
#특정 key에 관한 결과 검색 : lookup
keyword.lookup("s")

#RDD샘플 생성
import random
distinctChars = words.flatMap(lambda word : list(word.lower())).distinct().collect()
sampleMap = dict(map(lambda c:(c,random.random()),distinctChars))

words.map(lambda word:(word.lower()[0],word))\
  .sampleByKey(True,sampleMap,6).collect()

##Aggregation
chars = words.flatMap(lambda word:word.lower())
KVcharacters = chars.map(lambda letter : (letter,1))

#내부 조인
keyedChars = distinctChars.map(lambda c:(c,random.random()))
outputPartitions = 10

KVcharacters.join(keyedChars).count()
KVcharacters.join(keyedChars,outputPartitions).count()

#zip
numRange = sc.parallelize(range(10),2)
words.zip(numRange).collect()

#coalesce : 동일한 worker에 존재하는 partition을 합친다.
words.coalesce(1).getNumPartitions()

#repartition
words.repartition(10)
