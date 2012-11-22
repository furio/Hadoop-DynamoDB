package com.willetinc.hadoop.mapreduce.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.amazonaws.services.dynamodb.model.AttributeValue;

public class BSWritableTest {

	@Test
	public void testConstructorStringAttributeValue() {
		final byte[] bytes = new byte[] {0xA, 0xB, 0xC};
		final String FIELD_NAME = "table-name";
		
		AttributeValue value = new AttributeValue().withB(ByteBuffer.wrap(bytes));
		BWritable writable = new BWritable(FIELD_NAME, value) {};
		
		assertEquals(Types.BINARY, writable.getType());
		assertEquals(FIELD_NAME, writable.getFieldName());
		assertEquals(value, writable.getValue());
	}
	
	@Test
	public void testConstructorString() {
		final String FIELD_NAME = "table-name";
		
		BWritable writable = new BWritable(FIELD_NAME) {};
		
		assertEquals(Types.BINARY, writable.getType());
		assertEquals(FIELD_NAME, writable.getFieldName());
		assertNull(writable.getValue());
	}
}
