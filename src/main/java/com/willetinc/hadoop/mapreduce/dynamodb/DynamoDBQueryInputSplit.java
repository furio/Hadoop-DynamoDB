package com.willetinc.hadoop.mapreduce.dynamodb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;

public class DynamoDBQueryInputSplit extends DynamoDBInputSplit {
	private Types hashKeyType = Types.STRING;

	private AttributeValue hashKeyValue;

	private Types rangeKeyType = Types.STRING;

	private ComparisonOperator rangeKeyOperator = ComparisonOperator.EQ;

	private Collection<AttributeValue> rangeKeyValues = Collections
			.emptyList();
	
	private String hashKeyName;
	
	private String rangeKeyName;

	public DynamoDBQueryInputSplit() {

	}

	public DynamoDBQueryInputSplit(
			Types hashKeyType,
			AttributeValue hashKeyValue,
			String hashKeyName) {
		this.hashKeyType = hashKeyType;
		this.hashKeyValue = hashKeyValue;
		this.hashKeyName = hashKeyName;
	}

	public DynamoDBQueryInputSplit(
			Types hashKeyType,
			AttributeValue hashKeyValue,
			String hashKeyName,
			Types rangeKeyType,
			Collection<AttributeValue> rangeKeyValues,
			String rangeKeyName,
			ComparisonOperator rangeKeyOperator) {
		this.hashKeyType = hashKeyType;
		this.hashKeyValue = hashKeyValue;
		this.hashKeyName = hashKeyName;
		this.rangeKeyType = rangeKeyType;
		this.rangeKeyOperator = rangeKeyOperator;
		this.rangeKeyValues = rangeKeyValues;
		this.rangeKeyName = rangeKeyName;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.hashKeyType = Types.values()[in.readInt()];
		this.hashKeyValue = AttributeValueIOUtils.read(hashKeyType, in);
		this.hashKeyName = in.readLine();
		this.rangeKeyType = Types.values()[in.readInt()];
		this.rangeKeyValues =
				AttributeValueIOUtils.readCollection(rangeKeyType, in);
		this.rangeKeyName = in.readLine();
		this.rangeKeyOperator = ComparisonOperator.values()[in.readInt()];
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(hashKeyType.ordinal());
		AttributeValueIOUtils.write(hashKeyType, hashKeyValue, out);
		out.writeBytes(hashKeyName+"\n");
		out.writeInt(rangeKeyType.ordinal());
		AttributeValueIOUtils.writeCollection(
				rangeKeyType,
				rangeKeyValues,
				out);
		out.writeBytes(rangeKeyName+"\n");
		out.writeInt(rangeKeyOperator.ordinal());
	}

	public AttributeValue getHashKeyValue() {
		return hashKeyValue;
	}

	public Types getHashKeyType() {
		return hashKeyType;
	}

	public boolean hasHashKey() {
		return hashKeyValue != null;
	}
	
	public String getHashKeyName() {
		return hashKeyName;
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
	
	public String getRangeKeyName(){
		return rangeKeyName;
	}
	
	public Map<String, Condition> getKeyConditions() {
		Map<String, Condition> keyConditions = new HashMap<String, Condition>();
		Condition hashKeyCondition = new Condition()
			.withComparisonOperator(ComparisonOperator.EQ.toString())
			.withAttributeValueList(getHashKeyValue());
		
		keyConditions.put(hashKeyName, hashKeyCondition);
		
		// configure range key if it exists
		if(hasRangeKey()) {
			Condition rangeKeyCondition = new Condition()
				.withComparisonOperator(getRangeKeyOperator())
				.withAttributeValueList(getRangeKeyValues());
			keyConditions.put(rangeKeyName, rangeKeyCondition);
		}
		return keyConditions;
	}
}
