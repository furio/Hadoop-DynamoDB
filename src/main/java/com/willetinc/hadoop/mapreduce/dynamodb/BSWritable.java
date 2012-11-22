package com.willetinc.hadoop.mapreduce.dynamodb;

import com.amazonaws.services.dynamodb.model.AttributeValue;

public class BSWritable extends AttributeValueWritable {

	public BSWritable(String fieldName) {
		super(Types.BINARY_SET, fieldName);
	}

	public BSWritable(String fieldName, AttributeValue value) {
		super(Types.BINARY_SET, fieldName, value);
	}

}
