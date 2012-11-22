package com.willetinc.hadoop.mapreduce.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.amazonaws.services.dynamodb.model.AttributeValue;

public class SWritableTest {

	@Test
	public void testConstructorTypesStringAttributeValue() {
		final String VALUE = "test";
		final String FIELD_NAME = "table-name";
		
		AttributeValue value = new AttributeValue().withS(VALUE);
		SWritable writable = new SWritable(FIELD_NAME, value) {};
		
		assertEquals(Types.STRING, writable.getType());
		assertEquals(FIELD_NAME, writable.getFieldName());
		assertEquals(value, writable.getValue());
	}
	
	@Test
	public void testConstructorTypesString() {
		final String FIELD_NAME = "table-name";
		
		SWritable writable = new SWritable(FIELD_NAME) {};
		
		assertEquals(Types.STRING, writable.getType());
		assertEquals(FIELD_NAME, writable.getFieldName());
		assertNull(writable.getValue());
	}
}
