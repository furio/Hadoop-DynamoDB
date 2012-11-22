package com.willetinc.hadoop.mapreduce.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.amazonaws.services.dynamodb.model.AttributeValue;

public class AttributeValueWritableTest {

	@Test
	public void testConstructorTypesStringAttributeValue() {
		final String VALUE = "test";
		final String FIELD_NAME = "table-name";
		
		AttributeValue value = new AttributeValue().withS(VALUE);
		AttributeValueWritable writable = new AttributeValueWritable(Types.STRING, FIELD_NAME, value);
		
		assertEquals(Types.STRING, writable.getType());
		assertEquals(FIELD_NAME, writable.getFieldName());
		assertEquals(value, writable.getValue());
	}
	
	@Test
	public void testConstructorTypesString() {
		final String FIELD_NAME = "table-name";
		
		AttributeValueWritable writable = new AttributeValueWritable(Types.STRING, FIELD_NAME);
		
		assertEquals(Types.STRING, writable.getType());
		assertEquals(FIELD_NAME, writable.getFieldName());
		assertNull(writable.getValue());
	}
}
