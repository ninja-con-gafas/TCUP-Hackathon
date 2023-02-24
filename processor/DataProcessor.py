from json import load
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, date_trunc, lit, max, mean, min, round
from sys import argv, exit
from typing import Dict


def aggregate(streaming_data: DataFrame,
              batch: str) -> DataFrame:
    return streaming_data \
        .withColumn("timestamp_rx",
                    date_trunc(format=batch,
                               timestamp=col("timestamp_rx"))) \
        .groupby("timestamp_rx",
                 "device_name",
                 "device_type",
                 "description",
                 "unit") \
        .agg(max("measure").alias("maximum_measure"),
             round(mean("measure"), 3).alias("mean_measure"),
             min("measure").alias("minimum_measure")) \
        .withColumn("batch", lit(batch)) \
        .select("timestamp_rx",
                "device_name",
                "device_type",
                "description",
                "maximum_measure",
                "mean_measure",
                "minimum_measure",
                "unit",
                "batch")


class DataProcessor:

    def __init__(self, configuration_file_path):
        try:
            with open(file=configuration_file_path, mode='r') as configuration_file:
                configurations: Dict = load(configuration_file)
                spark_configuration: Dict = configurations.get("spark")
                mysql_configuration: Dict = configurations.get("mysql")
                table_names: Dict = mysql_configuration.get("table_names")
                environment_paths: Dict = configurations.get("environment_paths")

                self.app_name: str = spark_configuration.get("app_name")
                self.batches: Dict[str, bool] = spark_configuration.get("batches")
                self.verbose: bool = spark_configuration.get("verbose")

                self.database_url: str = mysql_configuration.get("database_url")
                self.user: str = mysql_configuration.get("user")
                self.password: str = mysql_configuration.get("password")
                self.streaming_data_table: str = table_names.get("streaming_data")
                self.processed_data_table: str = table_names.get("processed_data")

                self.mysql_connector_path: str = environment_paths.get("mysql_connector")

                self.spark: SparkSession = self.get_spark_session()
                self.spark_context: SparkContext = self.get_spark_context()
                self.spark_context.setLogLevel('ERROR')
        except IOError as error:
            print(f"Error opening the configuration file: {error}")

    def get_spark_session(self) -> SparkSession:
        return SparkSession \
            .builder \
            .appName(self.app_name) \
            .config(key="spark.jars",
                    value=self.mysql_connector_path) \
            .getOrCreate()

    def get_spark_context(self) -> SparkContext:
        return self.spark.sparkContext

    def read_streaming_data(self) -> DataFrame:
        return self.spark.read.format("jdbc") \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("url", self.database_url) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("dbtable", self.streaming_data_table) \
            .load()

    def ingest_processed_data(self, processed_data: DataFrame) -> None:
        processed_data.write.format("jdbc") \
            .option("driver", "com.mysql.jdbc.Driver") \
            .option("url", self.database_url) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("dbtable", self.processed_data_table) \
            .mode("append") \
            .save()

    def start(self) -> None:
        streaming_data = self.read_streaming_data().na.drop()
        for batch in [batch for batch, flag in self.batches.items() if flag]:
            aggregate_data = aggregate(streaming_data=streaming_data,
                                       batch=batch)
            self.ingest_processed_data(processed_data=aggregate_data)
            if self.verbose:
                aggregate_data.show(truncate=False)


if __name__ == "__main__":
    if len(argv) == 2:
        data_processor = DataProcessor(argv[1])
        data_processor.start()
    else:
        exit("Please provide a configuration file as command line argument.")
