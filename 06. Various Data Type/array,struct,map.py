
##복합데이터 타입
#struct
from pyspark.sql.functions import struct

complexDF=df.select(struct("Description","InvoiceNo").alias("complex"))
complexDF.select(col("complex").getField("Description")).show(3)

#array 생성 및 조회
from pyspark.sql.functions import split
df.select(split(col("Description")," ").alias("array_col")).selectExpr("array_col[0]").show(2)

#array_contain
from pyspark.sql.functions import array_contains

df.select(array_contains(split(col("Description")," "),"WHITE")).show(2)

#explode
from pyspark.sql.functions import explode
df.withColumn("Splitted",split(col("Description")," "))\
  .withColumn("exploded",explode(col("splitted")))\
  .select("Description","InvoiceNo","exploded").show(2)

#map 
from pyspark.sql.functions import create_map
df.select(create_map(col("Description"),col("InvoiceNo")).alias("complex_map")).show(3)


# COMMAND ----------


