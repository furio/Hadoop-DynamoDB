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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import java.nio.ByteBuffer;

import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.willetinc.hadoop.mapreduce.dynamodb.AttributeValueIOUtils;
import com.willetinc.hadoop.mapreduce.dynamodb.Types;

public class AttributeValueIOUtilsTest {
	
	@Test
	public void testToStringS() {
		final String VALUE = "test";
		AttributeValue attr = new AttributeValue().withS(VALUE);
		String result = AttributeValueIOUtils.toString(Types.STRING, attr);
		assertEquals(VALUE, result);
	}
	
	@Test
	public void testToStringN() {
		final String VALUE = "1234567890";
		AttributeValue attr = new AttributeValue().withN(VALUE);
		String result = AttributeValueIOUtils.toString(Types.NUMBER, attr);
		assertEquals(VALUE, result);
	}
	
	@Test
	public void testToStringB() {
		final byte[] BYTES = new byte[] {0xD, 0xE, 0xA, 0xD, 0xB, 0xE, 0xE, 0xF};
		final String ENCODED_VALUE = Base64.encodeBase64String(BYTES);
		
		ByteBuffer buf = ByteBuffer.wrap(BYTES);
		AttributeValue attr = new AttributeValue().withB(buf);
		String result = AttributeValueIOUtils.toString(Types.BINARY, attr);
		assertEquals(ENCODED_VALUE, result);
	}
	
	@Test
	public void testToStringNull() {
		assertNull(AttributeValueIOUtils.toString(Types.STRING, null));
	}
	
	@Test
	public void testValueOfS() {
		final String VALUE = "test";
		AttributeValue attr = AttributeValueIOUtils.valueOf(Types.STRING, VALUE);
		assertEquals(VALUE, attr.getS());
	}
	
	@Test
	public void testValueOfN() {
		final String VALUE = "1234567890";
		AttributeValue attr = AttributeValueIOUtils.valueOf(Types.NUMBER, VALUE);
		assertEquals(VALUE, attr.getN());
	}
	
	@Test
	public void testValueOfB() {
		final byte[] BYTES = new byte[] {0xD, 0xE, 0xA, 0xD, 0xB, 0xE, 0xE, 0xF};
		final String ENCODED_VALUE = Base64.encodeBase64String(BYTES);
		AttributeValue attr = AttributeValueIOUtils.valueOf(Types.BINARY, ENCODED_VALUE);
		assertArrayEquals(BYTES, attr.getB().array());
	}
	
	@Test
	public void testValueOfNull() {
		assertNull(AttributeValueIOUtils.valueOf(Types.BINARY, null));
	}

}
