package com.willetinc.hadoop.mapreduce.dynamodb;

import com.amazonaws.services.dynamodb.model.AttributeValue;

public class NSWritable extends AttributeValueWritable {

	public NSWritable(String fieldName) {
		super(Types.NUMBER_SET, fieldName);
	}

	public NSWritable(String fieldName, AttributeValue value) {
		super(Types.NUMBER_SET, fieldName, value);
	}

}
