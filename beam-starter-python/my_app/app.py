# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

from typing import Callable, Optional
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from beam_mysql.connector.io import WriteToMySQL, ReadFromMySQL
from beam_mysql.connector import splitters

from apache_beam.dataframe.convert import to_dataframe, to_pcollection
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib
import mysql.connector

db_connection = mysql.connector.connect(
    host="172.18.0.1",
    user="root",
    password="password",
    database="acme"
)

class MySQLInsert(beam.DoFn):
    def start_bundle(self):
        self.db_connection = mysql.connector.connect(
                                    host="172.18.0.1",
                                    user="root",
                                    password="password",
                                    database="acme"
                                )

    def process(self, element):
        cursor = self.db_connection.cursor()
        insert_query = "INSERT INTO new_badge (badge_serial_number, employee_id) VALUES (%s, (SELECT id FROM employee WHERE first_name = %s AND lastName = %s))"
        cursor.execute(insert_query, (element['badge_serial_number'], element['employee_first_name'], element['employee_last_name']))
        self.db_connection.commit()

    def finish_bundle(self):
        self.db_connection.close()

def run(
    input_text: str,
    beam_options: Optional[PipelineOptions] = None,
    test: Callable[[beam.PCollection], None] = lambda _: None,
) -> None:
    with beam.Pipeline(options=beam_options, runner='direct') as pipeline:
        csv_data = (pipeline
                | "Read CSV" >> beam.io.ReadFromText("../employees - infos badge à mettre à jour.csv", skip_header_lines=1)
                | "Split lines" >> beam.Map(lambda line: line.split(","))
                | "Create dictionary" >> beam.Map(lambda items: {
                    'employee_first_name': items[0],
                    'employee_last_name': items[1],
                    'badge_serial_number': items[2]
                })
                | "Insert into MySQL" >> beam.ParDo(MySQLInsert())
                )

db_connection.close()