# 사용자 정의 파티셔닝
df = spark.read.option("header", "true").option("inferSchema", "true") \
    .csv("/databricks-datasets/definitive-guide/data/retail-data/all/")
rdd = df.coalesce(10).rdd

df.printSchema()


def partitionFunc(key):
    import random

    if key == 17850 or 12583:
        return 0
    else:
        return random.randint(1, 2)


keyedRDD = rdd.keyBy(lambda row: row[6])
keyedRDD.take(5)

keyedRDD \
    .partitionBy(3, partitionFunc) \
    .map(lambda x: x[0]) \
    .glom() \
    .map(lambda x: len(set(x))) \
    .take(5)
