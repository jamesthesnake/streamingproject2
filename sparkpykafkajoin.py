from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue",StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr",BooleanType()),
        StructField("setsPerEntry", ArrayType( \
            StructType([
                StructField("element", StringType()),\
                StructField("score", StringType())   \
            ]))                                      \
        )

    ]
)

customerScheme = StructType(
    [
        StructField("custName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType())
    ]
)
stEvJson = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", StringType()),
        StructField("riskDate", StringType())
    ]
)

spark = SparkSession.builder.appName("my-final-project").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

redisServerRawStreamingDF = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe","redis-server")\
    .option("startingOffsets","earliest")\
    .load()

redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("cast (key as string) key", "cast (value as string) value")


redisServerStreamingDF.withColumn("value", from_json("value", redisMessageSchema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("RedisSortedSet")
setsPerEntrystreamingDFEncode = spark.sql("SELECT key, setsPerEntry[0].element as enCustomer FROM RedisSortedSet")


setsPerEntrystreamingDF = setsPerEntrystreamingDFEncode\
    .withColumn("enCustomer", unbase64(setsPerEntrystreamingDFEncode.enCustomer).cast("string"))
setsPerEntrystreamingDF.withColumn("decodedCustomer", from_json("enCustomer", customerScheme)).select(col("decodedCustomer.*")).createOrReplaceTempView("UserRec")

emailBirthstreamFrame = spark.sql("SELECT custName, email, birthDay FROM UserRec WHERE birthDay IS NOT NULL")

filteredBirthdayEmailDF = emailBirthstreamFrame.select('custName', 'email', 'birthDay', split(emailBirthstreamFrame.birthDay, "-").getItem(0).alias("birthYear"))
finalEmailBirthDF = filteredBirthdayEmailDF.select("birthYear", "email")

steidiFirstStream = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe","stedi-events")\
    .option("startingOffsets","earliest")\
    .load()
                                   
steidiFirstStreamSecondDF = steidiFirstStream.selectExpr("cast (key as string) key", "cast (value as string) value")
steidiFirstStreamSecondDF.withColumn("value", from_json("value", stEvJson)).select(col("value.*")).createOrReplaceTempView("RiskReview")

custRiskDF = spark.sql("SELECT customer, score FROM RiskReview")

joinStreamDFFinal = custRiskDF.join(finalEmailBirthDF, expr("""
customer = email
"""
))

joinStreamDFFinal.selectExpr("cast(customer as string) as key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "steidi-score") \
    .option("checkpointLocation","/tmp/kafkacheckpoint") \
    .start() \
    .awaitTermination()
