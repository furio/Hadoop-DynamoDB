package com.willetinc.hadoop.mapreduce.dynamodb;

import java.util.Map;

import org.apache.hadoop.io.Writable;

import com.amazonaws.services.dynamodb.model.AttributeValue;

public interface DynamoDBKeyWritable extends Writable {

	public Types getHashKeyType();

	public AttributeValue getHashKeyValue();

	public void setHashKeyValue(AttributeValue value);
	
	public boolean hasRangeKey();
	
	public Types setRangeKeyType();

	public AttributeValue getRangeKeyValue();

	public void setRangeKeyValue(AttributeValue value);
	
	public void readFields(Map<String, AttributeValue> in);
	
	public void write(Map<String, AttributeValue> out);
}
