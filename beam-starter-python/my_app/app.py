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

def run(
    input_text: str,
    beam_options: Optional[PipelineOptions] = None,
    test: Callable[[beam.PCollection], None] = lambda _: None,
) -> None:
    with beam.Pipeline(options=beam_options, runner=InteractiveRunner()) as pipeline:
        write_to_mysql = WriteToMySQL(
            host="localhost",
            database="acme",
            table="new_badge",
            user="root",
            password="password",
            port=3306,
            batch_size=100
        )

        read_from_mysql = ReadFromMySQL(
            query="SELECT * FROM acme.employee;",
            host="172.18.0.1",
            database="acme",
            user="root",
            password="password",
            port=3306,
            splitter=splitters.NoSplitter()  # you can select how to split query for performance
        )
        employee_collection = (
            pipeline
            | "read from mysqlt" >> read_from_mysql
            | "transform to row" >> beam.Map(lambda item: beam.Row(employee_id=item['id'], first_name=item['first_name'], 
                                                               last_name=item['lastName'], email=item['email'], group_id=item['group_id']))
            )
        # new_badge_infor_collection = (
        #     pipeline
        #     | "Read from text" >> beam.io.ReadFromText("../employees - infos badge à mettre à jour.csv")
        #     | "Split line" >> beam.Map(lambda item: item.split(","))
        #     | "to row" >> beam.Map(lambda item: beam.Row(first_name=item[0], last_name=item[1], serial_number=item[2]))
        # )

        employee_df = to_dataframe(employee_collection)
        # badge_df = to_dataframe(new_badge_infor_collection)
        badge_df = pipeline | beam.dataframe.io.read_csv("../employees - infos badge à mettre à jour.csv")
       
        new_df = employee_df.join(badge_df, lsuffix='_caller', rsuffix='_other').filter(items=['badge_serial_number', 'employee_id'])

        print(ib.collect(new_df))

        # to_pcollection(new_df) | "write to sql " >> write_to_mysql

        (
        pipeline
        | "ReadFromInMemory" >> beam.Create([{"badge_serial_number": '4180485c-ad12-45d0-a4e9-99b0db7a5f75'}, {"employee_id": 1}])
        | "NoTransform" >> beam.Map(lambda e: e)
        | "WriteToMySQL" >> write_to_mysql
        )

        # # Used for testing only.
        test(employee_collection)
