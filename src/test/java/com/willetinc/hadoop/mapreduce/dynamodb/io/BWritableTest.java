/**
 * Copyright 2012 Willet Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.willetinc.hadoop.mapreduce.dynamodb.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.nio.ByteBuffer;

import org.junit.Test;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.willetinc.hadoop.mapreduce.dynamodb.Types;

public class BWritableTest {

	@Test
	public void testConstructorStringAttributeValue() {
		final byte[] bytes = new byte[] {0xA, 0xB, 0xC};
		final String FIELD_NAME = "table-name";
		
		AttributeValue value = new AttributeValue().withBS(ByteBuffer.wrap(bytes));
		BSWritable writable = new BSWritable(FIELD_NAME, value) {};
		
		assertEquals(Types.BINARY_SET, writable.getType());
		assertEquals(FIELD_NAME, writable.getFieldName());
		assertEquals(value, writable.getValue());
	}
	
	@Test
	public void testConstructorString() {
		final String FIELD_NAME = "table-name";
		
		BSWritable writable = new BSWritable(FIELD_NAME) {};
		
		assertEquals(Types.BINARY_SET, writable.getType());
		assertEquals(FIELD_NAME, writable.getFieldName());
		assertNull(writable.getValue());
	}
}
