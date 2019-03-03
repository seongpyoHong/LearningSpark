from pyspark.sql.functions import date_format, col

#dataframe csv파일 불러오기
staticDataFrame = spark.read.format("csv")\
  .option("header","true")\
  .option("inferSchema", "true")\
  .load("/databricks-datasets/definitive-guide/data/retail-data/by-day/*.csv")

#schema 확인
staticDataFrame.printSchema()
#InvoiceDate를 수치형데이터(날짜 데이터)로 변환 
preppedDataFrame = staticDataFrame\
  .na.fill(0)\
  .withColumn("day_of_week", date_format(col("InvoiceDate"),"EEEE"))\
  .coalesce(5)

#학습 데이터 셋과 테스트 데이터 셋으로 분리
#특정 구매가 이루어진 날짜를 기준으로 직접 분리
trainDataFrame = preppedDataFrame\
  .where("InvoiceDate < '2011-07-01'")
testDataFrame = preppedDataFrame\
  .where("InvoiceDate >= '2011-07-01'")

#action호출 => 데이터 분리
trainDataFrame.count()
testDataFrame.count()

#StringIndexer 사용
#Transformation을 자동화
from pyspark.ml.feature import StringIndexer

#요일을 수치형으로 반환
indexer = StringIndexer()\
  .setInputCol("day_of_week")\
  .setOutputCol("day_of_week_index")

#OneHotEncoder 사용 : 각 값을 자체 컬럼으로 인코딩
from pyspark.ml.feature import OneHotEncoder

encoder = OneHotEncoder()\
  .setInputCol("day_of_week_index")\
  .setOutputCol("day_of_week_encoded")

#Spark에서 모든 머신러닝 알고리즘은 수치형 벡터 타입을 input으로 사용
from pyspark.ml.feature import VectorAssembler

#가격, 수량, 특정 날짜의 요일을 가지고 있는 vector 생성
vectorAssembler = VectorAssembler()\
  .setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"])\
  .setOutputCol("features")

#이후 입력값이 같은 과정을 거쳐 변환되도록 Pipeline 설정
from pyspark.ml import Pipeline

transformationPipeline = Pipeline()\
  .setStages([indexer, encoder, vectorAssembler])

#변환자(transformer)를 데이터셋에 fit시킨다.
fittedPipeline = transformationPipeline.fit(trainDataFrame)
#fittedPipeline으로 데이터 학습
transformedTraining = fittedPipeline.transform(trainDataFrame)
#캐싱을 사용하여 전체 파이프라인을 재실행하는 것 보다 빠르게 접근 가능
transformedTraining.cache()

#이 후 학습과정은 뒷부분에서 자세하게 진행.
