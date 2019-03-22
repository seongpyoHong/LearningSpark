# Databricks notebook source
##RDD
# Row 타입의 RDD 생성
spark.range(10).rdd
#데이터 처리
spark.range(10).toDF("ID").rdd.map(lambda row:row[0])

#데이터프레임 생성
spark.range(10).rdd.toDF()

#local collection으로 RDD 생성
#파티션 : 2개
myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple".split(" ")
words = spark.sparkContext.parallelize(myCollection,2)

#RDD 이름 지정
words.setName("mywords")
words.name()

#Data Source로 RDD 생성
spark.sparkContext.textFile("/c/users/ghtjd/desktop")

##Transformation
#distinct
words.distinct().count()
#filter
def startsWithS(individual):
  return individual.startswith('S')
words.filter(lambda word :startsWithS(word)).collect()

#map
words2 =words.map(lambda word: (word,word[0],word.startswith("S")))
words2.filter(lambda record : record[2]).take(5)

#flatMap
words.flatMap(lambda word:list(word)).take(5)

#sortBy
words.sortBy(lambda word: len(word)+1).take(5)

#randomSplit
fiftyFiftySplit = words.randomSplit([0.5,0.5])

##Action
#reduce
spark.sparkContext.parallelize(range(1,21)).reduce(lambda x,y:x+y)

def wordLength(left,right):
  if len(left)<len(right):
    return left
  else :
    return right
  
words.reduce(wordLength)

#count
words.count()

#countApprox
timeLimit = 400
confidence = 0.99
words.countApprox(timeLimit, confidence)

#countByValue
words.countByValue()

#first
words.first()

#min,max
spark.sparkContext.parallelize(range(20)).max()

#take
words.take(5)
withReplacement = True
numberToTake = 5
randomSeed = 100
words.takeSample(withReplacement,numberToTake,randomSeed)

##파일 저장
#saveAsTextFile
#words.saveAsTextFile("file:/tmp/bookTitle2")

#Sequence file
#words.saveAsObjectFile("file:/tmp/my/sequenceFilePath2")

## caching
words.cache()
words.getStorageLevel()

#checkpoint
spark.sparkContext.setCheckpointDir("/some/path/for/checkpointing")
words.checkpoint()

#RDD를 시스템 명령으로 전송
words.pipe("wc -1").collect()

#모든 파티션에 '1'값을 생성, 표현식에 따라 파티션 수를 세어 합산
words.mapPartitions(lambda part:[1]).sum()

#mapPartitionsWithIndex
def indexedFunc(partitionIndex, withPartIterator):
  return ["partition : {} => {}".format(partitionIndex,x) for x in withinPartIterator]

words.mapPartitionsWithIndex(indexedFunc).collect()

#foreachPartition
#words.foreachPartition { iter =>
#  import java.io._
#  import scala.util.Random
#  val randomFileName = new Random().nextInt()
#  val pw = new PrintWriter(new File(s"/tmp/random-file-${randomFileName}.txt"))
#  while (iter.hasNext) {
#      pw.write(iter.next())
#  }
#  pw.close()
#}

# COMMAND ----------



# COMMAND ----------


