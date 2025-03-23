from pyspark.sql import SparkSession

def create_spark_session():
    return SparkSession \
        .builder \
        .appName("KafkaConsolePrinter") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()


def read_from_kafka(spark, kafka_bootstrap_servers, kafka_topic):
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()


def process_kafka_stream(df):
    return df.selectExpr(
        "CAST(key AS STRING)",
        "CAST(value AS STRING)",
        "topic",
        "partition",
        "offset"
    )


def write_to_console(processed_df):
    query = processed_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    query.awaitTermination()


def main():
    # Configuration
    KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
    KAFKA_TOPIC = "users_click_events"

    spark = create_spark_session()
    kafka_df = read_from_kafka(spark, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    processed_df = process_kafka_stream(kafka_df)
    write_to_console(processed_df)

if __name__ == "__main__":
    main()
