package com.willetinc.hadoop.mapreduce.dynamodb;

import com.amazonaws.services.dynamodb.model.AttributeValue;

public class NWritable extends AttributeValueWritable {

	public NWritable(String fieldName) {
		super(Types.NUMBER, fieldName);
	}

	public NWritable(String fieldName, AttributeValue value) {
		super(Types.NUMBER, fieldName, value);
	}

}
