from datetime import datetime
from mqtt import MQTTUtils
from pyspark import RDD, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import ArrayType, IntegerType, LongType, StringType, StructField, StructType
from pyspark.streaming import DStream, StreamingContext
from typing import Tuple


def get_mysql_connector() -> str:
    return "venv/lib/python3.7/site-packages/pyspark/jars/mysql-connector-j-8.0.32.jar"


def get_credentials() -> Tuple[str, str]:
    user = "root"
    password = ""
    return user, password


def get_database_url() -> str:
    host: str = "127.0.0.1"
    port: str = "3306"
    data_base: str = "ICE_PLANT"
    tls_version: str = "TLSv1.2"
    return "jdbc:mysql://" + \
        host + \
        ":" + port + \
        "/" + data_base + \
        "?enabledTLSProtocols=" + tls_version


def get_stream_schema() -> StructType:
    return StructType(
        [
            StructField(name="data",
                        dataType=ArrayType(
                            elementType=StringType(),
                            containsNull=False),
                        nullable=False)
        ])


def get_payload_schema() -> StructType:
    return StructType(
        [
            StructField(name="timestamp",
                        dataType=StringType(),
                        nullable=False),
            StructField(name="metrics",
                        dataType=StringType(),
                        nullable=False),
            StructField(name="seq",
                        dataType=StringType(),
                        nullable=False),
            StructField(name="timestamp_rx",
                        dataType=LongType(),
                        nullable=False)
        ])


def get_metrics_schema() -> StructType:
    return StructType(
        [
            StructField(name="name",
                        dataType=StringType(),
                        nullable=False),
            StructField(name="timestamp",
                        dataType=StringType(),
                        nullable=False),
            StructField(name="datatype",
                        dataType=IntegerType(),
                        nullable=False),
            StructField(name="stringValue",
                        dataType=StringType(),
                        nullable=False),
            StructField(name="value",
                        dataType=StringType(),
                        nullable=False)
        ])


class DataProcessor:

    def __init__(self,
                 app_name: str,
                 batch_duration: int,
                 verbose: bool = False):
        self.app_name = app_name
        self.batch_duration = batch_duration
        self.verbose = verbose
        self.spark_context = self.get_spark_context()
        self.spark_streaming_context = self.create_spark_streaming_context()
        self.spark = self.get_spark_session()
        self.spark.conf.set(key="spark.jars",
                            value=get_mysql_connector())
        self.spark_context.setLogLevel('ERROR')

    def get_spark_context(self):
        return SparkContext(appName=self.app_name) \
            .getOrCreate()

    def create_spark_streaming_context(self):
        return StreamingContext(sparkContext=self.spark_context,
                                batchDuration=self.batch_duration)

    def get_spark_session(self):
        return SparkSession(sparkContext=self.spark_context)

    def create_mqtt_sparkplug_b_stream(self,
                                       broker_url: str,
                                       topic: str,
                                       username: str = None,
                                       password: str = None) -> DStream:
        return MQTTUtils.createStream(ssc=self.spark_streaming_context,
                                      brokerUrl=broker_url,
                                      topic=topic,
                                      username=username,
                                      password=password)

    def process(self,
                time: datetime,
                rdd: RDD) -> None:
        if not rdd.isEmpty():
            stream = self.spark \
                .createDataFrame(rdd.map(lambda data: Row(data))) \
                .select(from_json(col=col("_1"),
                                  schema=get_stream_schema()).alias("stream"))

            stream = stream \
                .withColumn("topic", stream["stream.data"].getItem(0)) \
                .withColumn("payload", from_json(col=stream["stream.data"].getItem(1),
                                                 schema=get_payload_schema())) \
                .withColumn("metrics", from_json(col=col("payload.metrics"),
                                                 schema=get_metrics_schema())) \
                .select("payload.timestamp",
                        "payload.timestamp_rx",
                        "topic",
                        "metrics.name",
                        "metrics.value")

            user, password = get_credentials()
            stream.write.format("jdbc") \
                .option("driver", "com.mysql.jdbc.Driver") \
                .option("url", get_database_url()) \
                .option("user", user) \
                .option("password", password) \
                .option("dbtable", "STREAMING_DATA") \
                .mode("append") \
                .save()
            if self.verbose:
                print(str(time))
                stream.show(truncate=False)

    def start(self,
              stream: DStream) -> None:
        stream.foreachRDD(self.process)
        self.spark_streaming_context.start()
        self.spark_streaming_context.awaitTermination()


if __name__ == "__main__":
    data_processor = DataProcessor(app_name="process_mqtt_sparkplug_b_data",
                                   batch_duration=1,
                                   verbose=True)
    mqtt_sparkplug_b_stream = data_processor.create_mqtt_sparkplug_b_stream(broker_url="tcp://127.0.0.1:1883",
                                                                            topic="ice_plant")
    data_processor.start(mqtt_sparkplug_b_stream)
