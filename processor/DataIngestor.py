from datetime import datetime
from json import load
from mqtt import MQTTUtils
from pyspark import RDD, SparkContext
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, from_json, from_utc_timestamp
from pyspark.sql.types import ArrayType, FloatType, IntegerType, LongType, StringType, StructField, StructType
from pyspark.streaming import DStream, StreamingContext
from sys import argv, exit
from typing import Dict


def get_stream_schema() -> StructType:
    return StructType(
        [
            StructField(name="data",
                        dataType=ArrayType(
                            elementType=StringType(),
                            containsNull=True),
                        nullable=True)
        ])


def get_payload_schema() -> StructType:
    return StructType(
        [
            StructField(name="timestamp",
                        dataType=StringType(),
                        nullable=True),
            StructField(name="metrics",
                        dataType=StringType(),
                        nullable=True),
            StructField(name="seq",
                        dataType=StringType(),
                        nullable=True),
            StructField(name="timestamp_rx",
                        dataType=LongType(),
                        nullable=True)
        ])


def get_metrics_schema() -> StructType:
    return StructType(
        [
            StructField(name="name",
                        dataType=StringType(),
                        nullable=True),
            StructField(name="timestamp",
                        dataType=StringType(),
                        nullable=True),
            StructField(name="datatype",
                        dataType=IntegerType(),
                        nullable=True),
            StructField(name="stringValue",
                        dataType=StringType(),
                        nullable=True),
            StructField(name="value",
                        dataType=StringType(),
                        nullable=True)
        ])


def get_value_schema() -> StructType:
    return StructType(
        [
            StructField(name="device_type",
                        dataType=StringType(),
                        nullable=True),
            StructField(name="description",
                        dataType=StringType(),
                        nullable=True),
            StructField(name="measure",
                        dataType=FloatType(),
                        nullable=True),
            StructField(name="unit",
                        dataType=StringType(),
                        nullable=True)
        ])


def create_stream_data_frame(spark: SparkSession,
                             rdd: RDD) -> DataFrame:
    return spark \
        .createDataFrame(rdd.map(lambda data: Row(data))) \
        .select(from_json(col=col("_1"),
                          schema=get_stream_schema()).alias("stream"))


def flatten_stream_data_frame(stream_data_frame: DataFrame) -> DataFrame:
    return stream_data_frame \
        .withColumn("topic", stream_data_frame["stream.data"].getItem(0)) \
        .withColumn("payload", from_json(col=stream_data_frame["stream.data"].getItem(1),
                                         schema=get_payload_schema())) \
        .withColumn("metrics", from_json(col=col("payload.metrics"),
                                         schema=get_metrics_schema())) \
        .withColumn("value", from_json(col=col("metrics.value"),
                                       schema=get_value_schema())) \
        .withColumn("timestamp",
                    from_utc_timestamp(timestamp=(col("metrics.timestamp") / 1000).cast("timestamp"),
                                       tz="GMT")) \
        .withColumn("timestamp_rx",
                    from_utc_timestamp(timestamp=(col("payload.timestamp_rx") / 1000).cast("timestamp"),
                                       tz="GMT")) \
        .select("timestamp",
                "timestamp_rx",
                "topic",
                "metrics.name",
                "value.device_type",
                "value.description",
                "value.measure",
                "value.unit") \
        .withColumnRenamed("name", "device_name")


class DataIngestor:

    def __init__(self, configuration_file_path):
        try:
            with open(file=configuration_file_path, mode='r') as configuration_file:
                configurations: Dict = load(configuration_file)
                spark_configuration: Dict = configurations.get("spark")
                mqtt_configuration: Dict = configurations.get("mqtt")
                mysql_configuration: Dict = configurations.get("mysql")
                environment_paths: Dict = configurations.get("environment_paths")

                self.app_name: str = spark_configuration.get("app_name")
                self.batch_duration: int = spark_configuration.get("batch_duration")
                self.verbose: bool = spark_configuration.get("verbose")

                self.broker_url: str = mqtt_configuration.get("broker_url")
                self.topic: str = mqtt_configuration.get("spb_group_name")

                self.database_url: str = mysql_configuration.get("database_url")
                self.user: str = mysql_configuration.get("user")
                self.password: str = mysql_configuration.get("password")
                self.table_name: str = mysql_configuration.get("table_name")

                mysql_connector_path: str = environment_paths.get("mysql_connector")

                self.spark_context: SparkContext = self.get_spark_context()
                self.spark_streaming_context: StreamingContext = self.create_spark_streaming_context()
                self.spark: SparkSession = self.get_spark_session()
                self.spark.conf.set(key="spark.jars",
                                    value=mysql_connector_path)
                self.spark_context.setLogLevel('ERROR')
        except IOError as error:
            print(f"Error opening the configuration file: {error}")

    def get_spark_context(self) -> SparkContext:
        return SparkContext(appName=self.app_name) \
            .getOrCreate()

    def create_spark_streaming_context(self) -> StreamingContext:
        return StreamingContext(sparkContext=self.spark_context,
                                batchDuration=self.batch_duration)

    def get_spark_session(self) -> SparkSession:
        return SparkSession(sparkContext=self.spark_context)

    def create_mqtt_sparkplug_b_stream(self) -> DStream:
        return MQTTUtils.createStream(ssc=self.spark_streaming_context,
                                      brokerUrl=self.broker_url,
                                      topic=self.topic,
                                      username=None,
                                      password=None)

    def ingest_stream_data_frame(self, stream_data_frame: DataFrame) -> None:
        stream_data_frame.write.format("jdbc") \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("url", self.database_url) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("dbtable", self.table_name) \
            .mode("append") \
            .save()

    def process(self,
                time: datetime,
                rdd: RDD) -> None:
        if not rdd.isEmpty():
            stream = create_stream_data_frame(spark=self.spark,
                                              rdd=rdd)
            stream = flatten_stream_data_frame(stream_data_frame=stream)
            self.ingest_stream_data_frame(stream_data_frame=stream)
            if self.verbose:
                print(str(time))
                stream.show(truncate=False)

    def start(self,
              stream: DStream) -> None:
        stream.foreachRDD(self.process)
        self.spark_streaming_context.start()
        self.spark_streaming_context.awaitTermination()


if __name__ == "__main__":
    if len(argv) == 2:
        data_ingestor = DataIngestor(argv[1])
        mqtt_sparkplug_b_stream = data_ingestor.create_mqtt_sparkplug_b_stream()
        data_ingestor.start(mqtt_sparkplug_b_stream)
    else:
        exit("Please provide a configuration file as command line argument.")
