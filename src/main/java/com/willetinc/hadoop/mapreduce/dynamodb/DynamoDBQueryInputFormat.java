package com.willetinc.hadoop.mapreduce.dynamodb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.amazonaws.services.dynamodb.model.ComparisonOperator;

public class DynamoDBQueryInputFormat<T extends DynamoDBKeyWritable> extends
		DynamoDBInputFormat<T> {

	public static class DynamoDBQueryInputSplit extends
			DynamoDBInputFormat.DynamoDBInputSplit {

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

		/** {@inheritDoc} */
		@Override
		public void readFields(DataInput in) throws IOException {
			this.hashKeyType = Types.values()[in.readInt()];
			this.hashKeyValue = AttributeValueIOUtils.read(hashKeyType, in);
			this.rangeKeyType = Types.values()[in.readInt()];
			this.rangeKeyValues =
					AttributeValueIOUtils.readCollection(rangeKeyType, in);
			this.rangeKeyOperator = ComparisonOperator.values()[in.readInt()];
		}

		/** {@inheritDoc} */
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
			return null;
		case NUMBER:
			new BigDecimalSplitter();
		case BINARY:
			return null;
		default:
			return new DefaultSplitter();
		}

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
