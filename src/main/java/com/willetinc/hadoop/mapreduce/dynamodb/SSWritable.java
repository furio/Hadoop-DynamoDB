package com.willetinc.hadoop.mapreduce.dynamodb;

import com.amazonaws.services.dynamodb.model.AttributeValue;

public abstract class SSWritable extends AttributeValueWritable {

	public SSWritable(String fieldName) {
		super(Types.STRING_SET, fieldName);
	}

	public SSWritable(String fieldName, AttributeValue value) {
		super(Types.STRING_SET, fieldName, value);
	}

}
