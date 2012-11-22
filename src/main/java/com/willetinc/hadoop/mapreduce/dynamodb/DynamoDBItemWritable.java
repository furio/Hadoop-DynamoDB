package com.willetinc.hadoop.mapreduce.dynamodb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import com.amazonaws.services.dynamodb.model.AttributeValue;

public class DynamoDBItemWritable implements DynamoDBKeyWritable {

	private String hashKeyFieldName;

	private Types hashKeyType;

	private AttributeValue hashKeyValue;

	private String rangeKeyFieldName;

	private Types rangeKeyType;

	private AttributeValue rangeKeyValue;

	protected DynamoDBItemWritable(
			String hashKeyFieldName,
			Types hashKeyType,
			String rangeKeyFieldName,
			Types rangeKeyType) {
		this.hashKeyFieldName = hashKeyFieldName;
		this.hashKeyType = hashKeyType;
		this.rangeKeyFieldName = rangeKeyFieldName;
		this.rangeKeyType = rangeKeyType;
	}

	protected DynamoDBItemWritable(String hashKeyFieldName, Types hashKeyType) {
		this.hashKeyFieldName = hashKeyFieldName;
		this.hashKeyType = hashKeyType;
	}

	@Override
	public Types getHashKeyType() {
		return hashKeyType;
	}

	@Override
	public AttributeValue getHashKeyValue() {
		return hashKeyValue;
	}

	@Override
	public void setHashKeyValue(AttributeValue hashKeyValue) {
		this.hashKeyValue = hashKeyValue;
	}

	@Override
	public boolean hasRangeKey() {
		return true;
	};

	@Override
	public Types setRangeKeyType() {
		return rangeKeyType;
	}

	@Override
	public AttributeValue getRangeKeyValue() {
		return rangeKeyValue;
	}

	@Override
	public void setRangeKeyValue(AttributeValue rangeKeyValue) {
		this.rangeKeyValue = rangeKeyValue;
	}

	@Override
	public final void readFields(Map<String, AttributeValue> in) {
		hashKeyValue = in.get(hashKeyFieldName);
		rangeKeyValue = in.get(rangeKeyFieldName);
		doReadFields(in);
	}

	@Override
	public final void readFields(DataInput in) throws IOException {
		hashKeyValue = AttributeValueIOUtils.read(hashKeyType, in);
		rangeKeyValue = AttributeValueIOUtils.read(rangeKeyType, in);
		doReadFields(in);
	}

	@Override
	public final void write(DataOutput out) throws IOException {
		AttributeValueIOUtils.write(hashKeyType, hashKeyValue, out);
		AttributeValueIOUtils.write(rangeKeyType, rangeKeyValue, out);
		doWrite(out);
	}

	@Override
	public final void write(Map<String, AttributeValue> out) {
		out.put(hashKeyFieldName, hashKeyValue);
		out.put(rangeKeyFieldName, rangeKeyValue);
		doWrite(out);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result =
				prime
						* result
						+ ((hashKeyValue == null) ? 0 : hashKeyValue.hashCode());
		result =
				prime
						* result
						+ ((rangeKeyValue == null) ? 0 : rangeKeyValue
								.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DynamoDBItemWritable other = (DynamoDBItemWritable) obj;
		if (hashKeyValue == null) {
			if (other.hashKeyValue != null)
				return false;
		} else if (!hashKeyValue.equals(other.hashKeyValue))
			return false;
		if (rangeKeyValue == null) {
			if (other.rangeKeyValue != null)
				return false;
		} else if (!rangeKeyValue.equals(other.rangeKeyValue))
			return false;
		return true;
	}

	protected void doReadFields(Map<String, AttributeValue> resultSet) {
	};

	protected void doReadFields(DataInput in) {
	};

	protected void doWrite(Map<String, AttributeValue> out) {
	};

	protected void doWrite(DataOutput out) {
	}

}
