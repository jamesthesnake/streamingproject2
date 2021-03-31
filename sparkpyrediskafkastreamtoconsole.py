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
customerTable = StructType(
    [
        StructField("custName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType())
    ]
)

spark = SparkSession.builder.appName("my-final-project").getOrCreate()
spark.sparkContext.setLogLevel('WARN')
rawRedis = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe","redis-server")\
    .option("startingOffsets","earliest")\
    .load()
rawRedisStreaming = rawRedis.selectExpr("cast (key as string) key", "cast (value as string) value")


rawRedisStreaming.withColumn("value", from_json("value", redisMessageSchema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("RedisSortedSet")
setsPerEntryTryDf = spark.sql("SELECT key, setsPerEntry[0].element as insertCustom FROM RedisSortedSet")

setsPerEntryDF = setsPerEntryTryDf\
    .withColumn("insertCustom", unbase64(setsPerEntryTryDf.insertCustom).cast("string"))
setsPerEntryDF.withColumn("decodedCustomer", from_json("insertCustom", customerTable)).select(col("decodedCustomer.*")).createOrReplaceTempView("CustomerRecords")

birthdayStreamemail = spark.sql("SELECT custName, email, birthDay FROM CustomerRecords WHERE birthDay IS NOT NULL")

importantBirthdayStreamemailDF = birthdayStreamemail.select('custName', 'email', 'birthDay', split(birthdayStreamemail.birthDay, "-").getItem(0).alias("birthYear"))
birthandEmail = importantBirthdayStreamemailDF.select("birthYear", "email")

birthandEmail.writeStream.outputMode("append").format("console").start().awaitTermination()
