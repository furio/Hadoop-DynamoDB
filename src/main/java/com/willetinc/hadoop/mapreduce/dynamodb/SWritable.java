package com.willetinc.hadoop.mapreduce.dynamodb;

import com.amazonaws.services.dynamodb.model.AttributeValue;

public class SWritable extends AttributeValueWritable {

	public SWritable(String fieldName) {
		super(Types.STRING, fieldName);
	}

	public SWritable(String fieldName, AttributeValue value) {
		super(Types.STRING, fieldName, value);
	}

}
