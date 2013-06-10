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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.willetinc.hadoop.mapreduce.dynamodb.io.AttributeValueWritable;
import com.willetinc.hadoop.mapreduce.dynamodb.io.DynamoDBKeyWritable;

public class DynamoDBScanInputFormat<T extends DynamoDBKeyWritable> extends
		InputFormat<LongWritable, T> implements Configurable {

	public static class NullDynamoDBWritable implements DynamoDBKeyWritable {

		@Override
		public void readFields(DataInput in) throws IOException {
		}

		@Override
		public void write(DataOutput out) throws IOException {
		}

		@Override
		public AttributeValueWritable getHashKey() {
			return null;
		}

		@Override
		public AttributeValue getHashKeyValue() {
			return null;
		}

		@Override
		public void setHashKeyValue(AttributeValue value) {
		}

		@Override
		public boolean hasRangeKey() {
			return false;
		}

		@Override
		public AttributeValueWritable getRangeKey() {
			return null;
		}

		@Override
		public AttributeValue getRangeKeyValue() {
			return null;
		}

		@Override
		public void setRangeKeyValue(AttributeValue value) {
		}

		@Override
		public void readFields(Map<String, AttributeValue> in) {
		}

		@Override
		public void write(Map<String, AttributeValue> out) {
		}
	}

	public static void setCredentials(
			Job job,
			String accessKey,
			String secretKey) {
		DynamoDBConfiguration.setCredentals(
				job.getConfiguration(),
				accessKey,
				secretKey);
	}
	
	public static void setEndpoint(Job job, String endpoint) {
		DynamoDBConfiguration dbConf = new DynamoDBConfiguration(job.getConfiguration());
		dbConf.setDynamoDBEndpoint(endpoint);
	}
	
	public static String getEndpoint(Job job) {
		DynamoDBConfiguration dbConf = new DynamoDBConfiguration(job.getConfiguration());
		return dbConf.getDynamoDBEndpoint();
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

	public String getTableName() {
		return tableName;
	}

	@Override
	public RecordReader<LongWritable, T> createRecordReader(
			InputSplit inputSplit,
			TaskAttemptContext context)
			throws IOException,
			InterruptedException {
		setConf(context.getConfiguration());

		@SuppressWarnings("unchecked")
		Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
		return new DynamoDBScanRecordReader<T>(
				(DynamoDBScanInputSplit) inputSplit,
				inputClass,
				context.getConfiguration(),
				dbConf.getAmazonDynamoDBClient(),
				dbConf,
				tableName);
	}

	public static void setInput(
			Job job,
			Class<? extends DynamoDBKeyWritable> inputClass,
			String tableName) {
		job.setInputFormatClass(DynamoDBScanInputFormat.class);
		DynamoDBConfiguration dbConf = new DynamoDBConfiguration(
				job.getConfiguration());
		dbConf.setInputClass(inputClass);
		dbConf.setInputTableName(tableName);
	}

	@Override
	public List<InputSplit> getSplits(JobContext job)
			throws IOException,
			InterruptedException {
		Configuration conf = job.getConfiguration();
		int numSplits = conf.getInt("mapred.map.tasks", 1);
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for(int i = 0; i < numSplits; i++) {
			splits.add(new DynamoDBScanInputSplit(numSplits, i));
		}
		return splits;
	}

}
