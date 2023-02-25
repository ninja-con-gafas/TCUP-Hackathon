from json import load
from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from sys import argv, exit
from typing import Dict


class DataFetcher:

    def __init__(self, configuration_file_path):
        try:
            with open(file=configuration_file_path, mode='r') as configuration_file:
                configurations: Dict = load(configuration_file)
                flask_configuration: Dict = configurations.get("flask")

                self.application = Flask(__name__)
                self.set_flask_application_configurations(flask_configuration
                                                          .get("configurations"))
                self.debug = flask_configuration.get("debug")

                self.sql_alchemy = self.get_sql_alchemy()
                self.table_name = flask_configuration.get("table_name")

        except IOError as error:
            print(f"Error opening the configuration file: {error}")

    def set_flask_application_configurations(self,
                                             configurations: Dict) -> None:
        for key, value in configurations.items():
            self.application.config[key] = value

    def get_sql_alchemy(self) -> SQLAlchemy:
        return SQLAlchemy(self.application)

    def get_processed_data_model(self):
        class ProcessedData(self.sql_alchemy.Model):
            __tablename__ = self.table_name
            timestamp_rx = self.sql_alchemy.Column("TIMESTAMP_RX",
                                                   self.sql_alchemy.DateTime,
                                                   primary_key=True)
            device_name = self.sql_alchemy.Column("DEVICE_NAME",
                                                  self.sql_alchemy.String(255),
                                                  primary_key=True)
            device_type = self.sql_alchemy.Column("DEVICE_TYPE",
                                                  self.sql_alchemy.String(255))
            description = self.sql_alchemy.Column("DESCRIPTION",
                                                  self.sql_alchemy.String(255))
            maximum_measure = self.sql_alchemy.Column("MAXIMUM_MEASURE",
                                                      self.sql_alchemy.Float)
            mean_measure = self.sql_alchemy.Column("MEAN_MEASURE",
                                                   self.sql_alchemy.Float)
            minimum_measure = self.sql_alchemy.Column("MINIMUM_MEASURE",
                                                      self.sql_alchemy.Float)
            unit = self.sql_alchemy.Column("UNIT",
                                           self.sql_alchemy.String(255))
            batch = self.sql_alchemy.Column("BATCH",
                                            self.sql_alchemy.String(255),
                                            primary_key=True)

            def serialize(self):
                return {
                    "TIMESTAMP_RX": self.timestamp_rx,
                    "DEVICE_NAME": self.device_name,
                    "DEVICE_TYPE": self.device_type,
                    "DESCRIPTION": self.description,
                    "MAXIMUM_MEASURE": self.maximum_measure,
                    "MEAN_MEASURE": self.mean_measure,
                    "MINIMUM_MEASURE": self.minimum_measure,
                    "UNIT": self.unit,
                    "BATCH": self.batch
                }

        return ProcessedData

    def define_api_calls(self):
        ProcessedData = self.get_processed_data_model()

        @self.application.route("/get_batch_data/batch/<batch>", methods=["GET"])
        def get_batch_data(batch: str):
            records = ProcessedData.query.filter_by(batch=batch).all()
            if records:
                return jsonify([record.serialize() for record in records])
            return jsonify(
                {
                    "Error": "Data not found."
                }
            )

    def run(self) -> None:
        self.application.run(debug=self.debug)


if __name__ == "__main__":
    if len(argv) == 2:
        application = DataFetcher(argv[1])
        application.define_api_calls()
        application.run()
    else:
        exit("Please provide a configuration file as command line argument.")
