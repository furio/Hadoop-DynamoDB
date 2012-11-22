package com.willetinc.hadoop.mapreduce.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.amazonaws.services.dynamodb.model.AttributeValue;

public class NSWritableTest {

	@Test
	public void testConstructorTypesStringAttributeValue() {
		final String VALUE = "077";
		final String FIELD_NAME = "table-name";
		
		AttributeValue value = new AttributeValue().withNS(VALUE);
		NSWritable writable = new NSWritable(FIELD_NAME, value);
		
		assertEquals(Types.NUMBER_SET, writable.getType());
		assertEquals(FIELD_NAME, writable.getFieldName());
		assertEquals(value, writable.getValue());
	}
	
	@Test
	public void testConstructorTypesString() {
		final String FIELD_NAME = "table-name";
		
		NSWritable writable = new NSWritable(FIELD_NAME);
		
		assertEquals(Types.NUMBER_SET, writable.getType());
		assertEquals(FIELD_NAME, writable.getFieldName());
		assertNull(writable.getValue());
	}
}
