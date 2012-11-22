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

package com.willetinc.hadoop.mapreduce.dynamodb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.amazonaws.services.dynamodb.model.AttributeValue;

public class NWritableTest {

	@Test
	public void testConstructorTypesStringAttributeValue() {
		final String VALUE = "077";
		final String FIELD_NAME = "table-name";
		
		AttributeValue value = new AttributeValue().withN(VALUE);
		NWritable writable = new NWritable(FIELD_NAME, value) {};
		
		assertEquals(Types.NUMBER, writable.getType());
		assertEquals(FIELD_NAME, writable.getFieldName());
		assertEquals(value, writable.getValue());
	}
	
	@Test
	public void testConstructorTypesString() {
		final String FIELD_NAME = "table-name";
		
		NWritable writable = new NWritable(FIELD_NAME) {};
		
		assertEquals(Types.NUMBER, writable.getType());
		assertEquals(FIELD_NAME, writable.getFieldName());
		assertNull(writable.getValue());
	}
}
