

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

# Get the spark application object
spark = SparkSession.builder.appName("my-final-project").getOrCreate()
# Set the spark log level to WARN
spark.sparkContext.setLogLevel('WARN')

stediJson = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", StringType()),
        StructField("riskDate", StringType())
    ]
)

steidiEventsStream = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe","stedi-events")\
    .option("startingOffsets","earliest")\
    .load()
steidiEventsFiltered = steidiEventsStream.selectExpr("cast (key as string) key", "cast (value as string) value")
steidiEventsFiltered.withColumn("value", from_json("value", stediJson)).select(col("value.*")).createOrReplaceTempView("RiskReview")

customerRiskFilteredSpark = spark.sql("SELECT customer, score FROM RiskReview")
customerRiskFilteredSpark.writeStream.outputMode("append").format("console").start().awaitTermination()
