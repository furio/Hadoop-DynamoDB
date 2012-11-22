/**
 * Copyright 2012 Willet Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.willetinc.hadoop.mapreduce.dynamodb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.willetinc.hadoop.mapreduce.dynamodb.io.DynamoDBKeyWritable;

public class DynamoDBInputFormat<T extends DynamoDBKeyWritable>
		extends InputFormat<LongWritable, T> implements Configurable {

	public static class NullDynamoDBWritable implements DBWritable, Writable {

		public void readFields(DataInput in) throws IOException {
		}

		public void write(DataOutput out) throws IOException {
		}

		public void readFields(ResultSet resultSet) throws SQLException {
		}

		public void write(PreparedStatement statement) throws SQLException {
		}
	}

	public static class DynamoDBInputSplit extends InputSplit implements
			Writable {

		/**
		 * Default Constructor
		 */
		public DynamoDBInputSplit() {
		}

		public void readFields(DataInput in) throws IOException {

		}

		public void write(DataOutput out) throws IOException {

		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			return new String[] {};
		}

	}

	private DynamoDBConfiguration dbConf;
	
	private String tableName;

	public Configuration getConf() {
		return dbConf.getConf();
	}

	public void setConf(Configuration conf) {
		dbConf = new DynamoDBConfiguration(conf);
		tableName = dbConf.getInputTableName();
	}
	
	public DynamoDBConfiguration getDBConf() {
		return dbConf;
	}

	@Override
	public RecordReader<LongWritable, T> createRecordReader(InputSplit inputSplit,
			TaskAttemptContext context) throws IOException, InterruptedException {
		
		@SuppressWarnings("unchecked")
		Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
		return new DynamoDBScanRecordReader<T>(
				(DynamoDBInputSplit) inputSplit,
				inputClass, 
				context.getConfiguration(), 
				dbConf.getAmazonDynamoDBClient(), 
				dbConf, 
				dbConf.getInputFieldNames(), 
				tableName);
	}
	
	public static void setInput(Job job, 
			Class<? extends DynamoDBKeyWritable> inputClass,
			String tableName,String conditions, 
			String orderBy, String... fieldNames) {
		job.setInputFormatClass(DynamoDBInputFormat.class);
		DynamoDBConfiguration dbConf = new DynamoDBConfiguration(job.getConfiguration());
		dbConf.setInputClass(inputClass);
		dbConf.setInputTableName(tableName);
		dbConf.setInputFieldNames(fieldNames);
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		splits.add(new DynamoDBInputSplit());
		return splits;
	}

}
