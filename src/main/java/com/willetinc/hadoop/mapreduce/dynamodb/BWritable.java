package com.willetinc.hadoop.mapreduce.dynamodb;

import com.amazonaws.services.dynamodb.model.AttributeValue;

public class BWritable extends AttributeValueWritable {

	public BWritable(String fieldName) {
		super(Types.BINARY, fieldName);
	}

	public BWritable(String fieldName, AttributeValue value) {
		super(Types.BINARY, fieldName, value);
	}

}
