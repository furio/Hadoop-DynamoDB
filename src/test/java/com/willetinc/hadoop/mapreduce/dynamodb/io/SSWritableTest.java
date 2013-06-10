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

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.willetinc.hadoop.mapreduce.dynamodb.Types;

public class SSWritableTest {

	@Test
	public void testConstructorTypesStringAttributeValue() {
		final String VALUE = "test";
		final String FIELD_NAME = "table-name";
		
		AttributeValue value = new AttributeValue().withSS(VALUE);
		SSWritable writable = new SSWritable(FIELD_NAME, value) {};
		
		assertEquals(Types.STRING_SET, writable.getType());
		assertEquals(FIELD_NAME, writable.getFieldName());
		assertEquals(value, writable.getValue());
	}
	
	@Test
	public void testConstructorTypesString() {
		final String FIELD_NAME = "table-name";
		
		SSWritable writable = new SSWritable(FIELD_NAME) {};
		
		assertEquals(Types.STRING_SET, writable.getType());
		assertEquals(FIELD_NAME, writable.getFieldName());
		assertNull(writable.getValue());
	}
}
