#Low Level API

from pyspark.sql import Row
spark.sparkContext.parallelize([Row(1),Row(2),Row(3)]).toDF()

