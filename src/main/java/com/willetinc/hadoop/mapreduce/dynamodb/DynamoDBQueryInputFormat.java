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
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;
import com.willetinc.hadoop.mapreduce.dynamodb.io.DynamoDBKeyWritable;

public class DynamoDBQueryInputFormat<T extends DynamoDBKeyWritable> 
	extends InputFormat<LongWritable, T> implements Configurable {

	public static class DynamoDBQueryInputSplit extends
			DynamoDBScanInputFormat.DynamoDBInputSplit {

		private Types hashKeyType = Types.STRING;

		private AttributeValue hashKeyValue;

		private Types rangeKeyType = Types.STRING;

		private ComparisonOperator rangeKeyOperator = ComparisonOperator.EQ;

		private Collection<AttributeValue> rangeKeyValues = Collections
				.emptyList();

		public DynamoDBQueryInputSplit() {

		}

		public DynamoDBQueryInputSplit(
				Types hashKeyType,
				AttributeValue hashKeyValue) {
			this.hashKeyType = hashKeyType;
			this.hashKeyValue = hashKeyValue;
		}

		public DynamoDBQueryInputSplit(
				Types hashKeyType,
				AttributeValue hashKeyValue,
				Types rangeKeyType,
				Collection<AttributeValue> rangeKeyValues,
				ComparisonOperator rangeKeyOperator) {

			this.hashKeyType = hashKeyType;
			this.hashKeyValue = hashKeyValue;
			this.rangeKeyType = rangeKeyType;
			this.rangeKeyOperator = rangeKeyOperator;
			this.rangeKeyValues = rangeKeyValues;
		}

		/**
		 * @return The total row count in this split
		 */
		@Override
		public long getLength() throws IOException {
			return 0; // unfortunately, we don't know this.
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			this.hashKeyType = Types.values()[in.readInt()];
			this.hashKeyValue = AttributeValueIOUtils.read(hashKeyType, in);
			this.rangeKeyType = Types.values()[in.readInt()];
			this.rangeKeyValues =
					AttributeValueIOUtils.readCollection(rangeKeyType, in);
			this.rangeKeyOperator = ComparisonOperator.values()[in.readInt()];
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(hashKeyType.ordinal());
			AttributeValueIOUtils.write(hashKeyType, hashKeyValue, out);
			out.writeInt(rangeKeyType.ordinal());
			AttributeValueIOUtils.writeCollection(
					rangeKeyType,
					rangeKeyValues,
					out);
			out.writeInt(rangeKeyOperator.ordinal());
		}

		public AttributeValue getHashKeyValue() {
			return hashKeyValue;
		}

		public Types getRangeType() {
			return rangeKeyType;
		}

		public Types getHashKeyType() {
			return hashKeyType;
		}

		public boolean hasHashKey() {
			return hashKeyValue != null;
		}

		public Types getRangeKeyType() {
			return rangeKeyType;
		}

		public boolean hasRangeKey() {
			if (rangeKeyValues == null)
				return false;
			return rangeKeyValues.size() > 0;
		}

		public ComparisonOperator getRangeKeyOperator() {
			return rangeKeyOperator;
		}

		public Collection<AttributeValue> getRangeKeyValues() {
			return rangeKeyValues;
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
	
	public String getTableName() {
		return tableName;
	}
	
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		Configuration conf = job.getConfiguration();
		Types rangeKeyType = DynamoDBQueryInputFormat.getRangeKeyType(conf);

		DynamoDBSplitter splitter = getSplitter(rangeKeyType);
		return splitter.split(conf);
	}

	protected DynamoDBSplitter getSplitter(Types rangeKeyType) {
		switch (rangeKeyType) {
		case STRING:
			return new TextSplitter();
		case NUMBER:
			new BigDecimalSplitter();
		case BINARY:
			return new BinarySplitter();
		default:
			return new DefaultSplitter();
		}
	}
	
	@Override
	public RecordReader<LongWritable, T> createRecordReader(InputSplit inputSplit,
			TaskAttemptContext context) throws IOException, InterruptedException {
		setConf(context.getConfiguration());
		
		@SuppressWarnings("unchecked")
		Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
		return new DynamoDBQueryRecordReader<T>(
				(DynamoDBQueryInputSplit) inputSplit,
				inputClass, 
				context.getConfiguration(), 
				dbConf.getAmazonDynamoDBClient(), 
				dbConf, 
				tableName);
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
	
	public static void setInput(Job job, 
			Class<? extends DynamoDBKeyWritable> inputClass,
			String tableName) {
		job.setInputFormatClass(DynamoDBQueryInputFormat.class);
		DynamoDBConfiguration dbConf = new DynamoDBConfiguration(job.getConfiguration());
		dbConf.setInputClass(inputClass);
		dbConf.setInputTableName(tableName);
	}

	public static Types getHashKeyType(Configuration conf) {
		return Types.values()[conf.getInt(
				DynamoDBConfiguration.HASH_KEY_TYPE_PROPERTY,
				Types.STRING.ordinal())];
	}

	public static void setHashKeyType(Configuration conf, Types type) {
		conf.setInt(
				DynamoDBConfiguration.HASH_KEY_TYPE_PROPERTY,
				type.ordinal());
	}

	public static AttributeValue getHashKeyValue(Configuration conf) {
		String value = conf.get(DynamoDBConfiguration.HASH_KEY_VALUE_PROPERTY);
		return AttributeValueIOUtils.valueOf(getHashKeyType(conf), value);
	}

	public static void setHashKeyValue(
			Configuration conf,
			Types type,
			AttributeValue value) {
		setHashKeyType(conf, type);
		String encodedValue = AttributeValueIOUtils.toString(type, value);
		conf.set(DynamoDBConfiguration.HASH_KEY_VALUE_PROPERTY, encodedValue);
	}

	public static
			boolean
			getInterpolateAcrossRangeKeyValues(Configuration conf) {
		return conf.getBoolean(
				DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_PROPERTY,
				false);
	}

	public static void setInterpolateAcrossRangeKeyValues(
			Configuration conf,
			boolean interpolate) {
		conf.setBoolean(
				DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_PROPERTY,
				interpolate);
	}

	public static Types getRangeKeyType(Configuration conf) {
		return Types.values()[conf.getInt(
				DynamoDBConfiguration.RANGE_KEY_TYPE_PROPERTY,
				Types.STRING.ordinal())];
	}

	public static void setRangeKeyType(Configuration conf, Types type) {
		conf.setInt(
				DynamoDBConfiguration.RANGE_KEY_TYPE_PROPERTY,
				type.ordinal());
	}

	public static void setRangeKeyValues(
			Configuration conf,
			Types type,
			Collection<AttributeValue> values) {
		setInterpolateAcrossRangeKeyValues(conf, false);
		setRangeKeyType(conf, type);
		List<String> attrValues = new ArrayList<String>();
		for (AttributeValue attr : values) {
			attrValues.add(AttributeValueIOUtils.toString(type, attr));
		}

		conf.setStrings(
				DynamoDBConfiguration.RANGE_KEY_VALUES_PROPERTY,
				attrValues.toArray(new String[] {}));
	}

	public static Collection<AttributeValue> getRangeKeyValues(
			Configuration conf) {
		List<AttributeValue> values = new ArrayList<AttributeValue>();
		Types type = getRangeKeyType(conf);
		String[] encodedValues =
				conf.getStrings(DynamoDBConfiguration.RANGE_KEY_VALUES_PROPERTY);

		// if range key values have not been configured return
		if (null == encodedValues)
			return values;

		// decode values
		for (String encodedValue : encodedValues) {
			values.add(AttributeValueIOUtils.valueOf(type, encodedValue));
		}

		return values;
	}

	public static ComparisonOperator getRangeKeyComparisonOperator(
			Configuration conf) {
		return ComparisonOperator.values()[conf.getInt(
				DynamoDBConfiguration.RANGE_KEY_OPERATOR_PROPERTY,
				ComparisonOperator.EQ.ordinal())];
	}

	public static void setRangeKeyComparisonOperator(
			Configuration conf,
			ComparisonOperator operator) {
		conf.setInt(
				DynamoDBConfiguration.RANGE_KEY_OPERATOR_PROPERTY,
				operator.ordinal());
	}

	public static void setRangeKeyCondition(
			Configuration conf,
			Types type,
			ComparisonOperator operator,
			Collection<AttributeValue> values) {
		setRangeKeyComparisonOperator(conf, operator);
		setRangeKeyValues(conf, type, values);
	}

	public static void setRangeKeyInterpolateMinValue(
			Configuration conf,
			Types type,
			AttributeValue value) {
		setInterpolateAcrossRangeKeyValues(conf, true);
		setRangeKeyType(conf, type);
		String encodedValue = AttributeValueIOUtils.toString(type, value);
		conf.set(
				DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_MIN_VALUE_PROPERTY,
				encodedValue);
	}

	public static AttributeValue getRangeKeyInterpolateMinValue(
			Configuration conf) {
		Types type = getRangeKeyType(conf);
		String encodedValue =
				conf.get(DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_MIN_VALUE_PROPERTY);
		return AttributeValueIOUtils.valueOf(type, encodedValue);
	}

	public static void setRangeKeyInterpolateMaxValue(
			Configuration conf,
			Types type,
			AttributeValue value) {
		setInterpolateAcrossRangeKeyValues(conf, true);
		setRangeKeyType(conf, type);
		String encodedValue = AttributeValueIOUtils.toString(type, value);
		conf.set(
				DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_MAX_VALUE_PROPERTY,
				encodedValue);
	}

	public static AttributeValue getRangeKeyInterpolateMaxValue(
			Configuration conf) {
		Types type = getRangeKeyType(conf);
		String encodedValue =
				conf.get(DynamoDBConfiguration.RANGE_KEY_INTERPOLATE_MAX_VALUE_PROPERTY);
		return AttributeValueIOUtils.valueOf(type, encodedValue);
	}

	public static void setRangeKeyInterpolateRange(
			Configuration conf,
			Types type,
			AttributeValue minValue,
			AttributeValue maxValue) {
		setRangeKeyInterpolateMinValue(conf, type, minValue);
		setRangeKeyInterpolateMaxValue(conf, type, maxValue);
	}
}
