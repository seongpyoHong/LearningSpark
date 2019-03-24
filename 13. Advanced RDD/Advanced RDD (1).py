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

def maxFunc(left,right):
  return max(left,right)
def addFunc(left,rifht):
  return left+right

nums = sc.parallelize(range(1,31),5)
#countByKey
KVcharacters.countByKey()

from functools import reduce
#groupByKey
KVcharacters.groupByKey().map(lambda row : (row[0],reduce(addFunc,row[1]))).collect()

#reduceByKey
KVcharacters.reduceByKey(addFunc).collect()

#aggregate
nums.aggregate(0,maxFunc,addFunc)

#treeAggregate
nums.treeAggregate(0,maxFunc,addFunc,depth)

#aggregateByKey
KVcharacters.aggregateByKey(0,addFunc,maxFunc).collect()

#cogroup
charRDD = distinctChars.map(lambda c : (c,random.random()))
charRDD2 = distinctChars.map(lambda c : (c,random.random()))
charRDD.cogroup(charRDD2).take(5)

# COMMAND ----------



# COMMAND ----------


