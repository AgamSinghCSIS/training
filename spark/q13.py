from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
from lib.logger import Log4j


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("q-13") \
        .getOrCreate()

    logger = Log4j(spark)

    schema = StructType([
        StructField("name", StringType()),
        StructField("sku", StringType()),
        StructField("price", DoubleType())
    ])
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe","q13-kafka") \
        .option("startingOffsets", "earliest") \
        .load()

    final_df = (kafka_df.select(from_json(col("value").cast("string"), schema).alias("value")))
    final_df.printSchema()
    sQuery = final_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

    sQuery.awaitTermination()
