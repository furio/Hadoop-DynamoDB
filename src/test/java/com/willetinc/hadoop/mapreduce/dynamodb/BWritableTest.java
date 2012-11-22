package com.willetinc.hadoop.mapreduce.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.amazonaws.services.dynamodb.model.AttributeValue;

public class BWritableTest {

	@Test
	public void testConstructorStringAttributeValue() {
		final byte[] bytes = new byte[] {0xA, 0xB, 0xC};
		final String FIELD_NAME = "table-name";
		
		AttributeValue value = new AttributeValue().withBS(ByteBuffer.wrap(bytes));
		BSWritable writable = new BSWritable(FIELD_NAME, value);
		
		assertEquals(Types.BINARY_SET, writable.getType());
		assertEquals(FIELD_NAME, writable.getFieldName());
		assertEquals(value, writable.getValue());
	}
	
	@Test
	public void testConstructorString() {
		final String FIELD_NAME = "table-name";
		
		BSWritable writable = new BSWritable(FIELD_NAME);
		
		assertEquals(Types.BINARY_SET, writable.getType());
		assertEquals(FIELD_NAME, writable.getFieldName());
		assertNull(writable.getValue());
	}
}
