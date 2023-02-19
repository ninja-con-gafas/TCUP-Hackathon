from datetime import datetime
from mqtt import MQTTUtils
from pyspark import RDD, SparkContext
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import ArrayType, IntegerType, LongType, StringType, StructField, StructType
from pyspark.streaming import DStream, StreamingContext


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
        self.stream = self.spark.createDataFrame([], StructType([]))
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

    def create_stream_data_frame(self,
                                 time: datetime,
                                 rdd: RDD) -> None:
        if not rdd.isEmpty():
            self.stream = self.spark \
                .createDataFrame(rdd.map(lambda stream: Row(stream))) \
                .select(from_json(col=col("_1"),
                                  schema=get_stream_schema()).alias("stream"))

            self.stream = self.stream \
                .withColumn("topic", self.stream["stream.data"].getItem(0)) \
                .withColumn("payload", from_json(col=self.stream["stream.data"].getItem(1),
                                                 schema=get_payload_schema())) \
                .withColumn("metrics", from_json(col=col("payload.metrics"),
                                                 schema=get_metrics_schema())) \
                .select("topic",
                        "payload.timestamp",
                        "payload.timestamp_rx",
                        "payload.seq",
                        "metrics.name",
                        "metrics.value")
            if self.verbose:
                print(str(time))
                self.stream.show(truncate=False)

    def process(self,
                stream: DStream) -> None:
        stream.foreachRDD(self.create_stream_data_frame)
        self.spark_streaming_context.start()
        self.spark_streaming_context.awaitTermination()


if __name__ == "__main__":
    data_processor = DataProcessor(app_name="process_mqtt_sparkplug_b_data",
                                   batch_duration=1,
                                   verbose=True)
    mqtt_sparkplug_b_stream = data_processor.create_mqtt_sparkplug_b_stream(broker_url="tcp://127.0.0.1:1883",
                                                                            topic="ice_plant")
    data_processor.process(mqtt_sparkplug_b_stream)
