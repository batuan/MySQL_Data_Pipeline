// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

package com.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class App {
	public interface Options extends StreamingOptions {
		@Description("Input text to print.")
		@Default.String("My input text")
		String getInputText();

		void setInputText(String value);
		String getInputMySQLHostName();
		void setInputMySQLHostName(String value);

		String getInputMySQLTable();
		void setInputMySQLTable(String value);

		String getInputInfoBadgeCSVPath();
		void setInputInfoBadgeCSVPath(String value);

		String getInputMySQLUserName();
		void setInputMySQLUserName(String value);

		String getInputMySQLPassword();
		void setInputMySQLPassword(String value);
	}

	static class CSVSplitFn extends DoFn<String, KV<String, String>> {
		@ProcessElement
		public void processElement(ProcessContext c) {
			if (!c.element().startsWith("employee_first_name")) {
				String[] splitValues = c.element().split(",");
				c.output(KV.of(splitValues[0] + "," + splitValues[1], splitValues[2]));
			}
		}
	}

	public static PCollection<@UnknownKeyFor @NonNull @Initialized String> buildPipeline(Pipeline pipeline, Options options) {
		String queryEmployee = "select id, first_name, lastName\n" +
				"from employee\n" +
				"where (first_name, lastName) IN (\n" +
				"SELECT first_name, lastName\n" +
				"    FROM employee\n" +
				"    GROUP BY first_name, lastName\n" +
				"    having count(*)=1)";

		PCollection<KV<String, String>> leftPcollection = pipeline.apply("read from text", TextIO.read()
						.from(options.getInputInfoBadgeCSVPath()))
				.apply(ParDo.of(new CSVSplitFn()));

		JdbcIO.DataSourceConfiguration mySQLSourceConfiguration = JdbcIO.DataSourceConfiguration.create(
						"com.mysql.cj.jdbc.Driver", String.format("jdbc:mysql://%s:3306/acme", options.getInputMySQLHostName()))
				.withUsername(options.getInputMySQLUserName()).withPassword(options.getInputMySQLPassword());

		PCollection<KV<String, Integer>> rightPcollection = pipeline
				.apply("connect mysql", JdbcIO.<KV<String, Integer>>read()
						.withDataSourceConfiguration(mySQLSourceConfiguration)
						.withQuery(queryEmployee)
						.withRowMapper(new JdbcIO.RowMapper<KV<String, Integer>>() {
							public KV<String, Integer> mapRow(ResultSet resultSet) throws Exception {
								return KV.of(resultSet.getString(2) + "," + resultSet.getString(3), resultSet.getInt(1));
							}
						})
				);

		PCollection<KV<String, KV<String, Integer>>> joinedPcollection = Join.innerJoin(leftPcollection, rightPcollection);
		joinedPcollection.apply(JdbcIO.<KV<String, KV<String, Integer>>>write().withDataSourceConfiguration(mySQLSourceConfiguration)
				.withStatement("insert into badge(serial_number, employee_id) values(?,?)")
				.withPreparedStatementSetter(new JdbcIO.PreparedStatementSetter<KV<String, KV<String, Integer>>>() {
					@Override
					public void setParameters(KV<String, KV<String, Integer>> element, @UnknownKeyFor @NonNull @Initialized PreparedStatement query) throws @UnknownKeyFor@NonNull@Initialized Exception {
						query.setString(1, element.getValue().getKey());
						query.setInt(2, element.getValue().getValue());
					}
				}));

		return joinedPcollection.apply("Print elements",
				MapElements.into(TypeDescriptors.strings()).via(x -> {
//					System.out.println(x.getKey() + ", " + x.getValue().getKey() + ", " + x.getValue().getValue());
					return String.valueOf(x);
				}));
	}

	public static void main(String[] args) {
		var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		var pipeline = Pipeline.create(options);
		System.out.println("===================================================");
		System.out.println("CSV file Path: " + options.getInputInfoBadgeCSVPath());
		System.out.println("===================================================");
		System.out.printf("Connect to Database: jdbc:mysql://%s:3306/acme%n", options.getInputMySQLHostName());
		System.out.println("===================================================");

		App.buildPipeline(pipeline, options);
		pipeline.run().waitUntilFinish();
	}
}