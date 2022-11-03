from pyspark.sql.functions import mean, max, min, col, count
from pyspark.sql import SparkSession

spark = (
   SparkSession.builder.appName("ExerciseSpark")
   .getOrCreate()
)

enem = (
	spark
	.read
	.format("csv")
	.option("header", True)
	.option("inferSchema", True)
    .option("delimiter",";")
	.load("s3://datalake-fernandomelo-649506824625/raw-data/enem/microdados/")
)

(
   enem
   .write("overwrite")
   .format("parquet")
   .partitionBy("year")
   .save("s3://datalake-fernandomelo-649506824625/consumer-zone/enem")
)