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

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class BinarySplitterTest {

	@Test
	public void testSplitInteger() {

		final int NUM_RANGE_SPLITS = 2;
		final String VALUE = "007";
		final Types hashKeyType = Types.NUMBER;
		final AttributeValue hashKeyValue = new AttributeValue().withN(VALUE);
		final String hashKeyName = "Id";
		final Types rangeKeyType = Types.STRING;
		final AttributeValue minRangeKeyValue =
				new AttributeValue().withB(ByteBuffer.wrap(new byte[] {0x0, 0x0}));
		final AttributeValue maxRangeKeyValue =
				new AttributeValue().withB(ByteBuffer.wrap(new byte[] {0x0, 0xF}));
		final String rangeKeyName = "range";

		Configuration conf = createMock(Configuration.class);
		BinarySplitter splitter = new BinarySplitter();

		List<InputSplit> inputSplits = new ArrayList<InputSplit>();

		splitter.generateRangeKeySplits(
				conf,
				inputSplits,
				hashKeyType,
				hashKeyValue,
				hashKeyName,
				rangeKeyType,
				minRangeKeyValue,
				maxRangeKeyValue,
				rangeKeyName,
				NUM_RANGE_SPLITS);

		assertEquals(2, inputSplits.size());

		
		for (InputSplit inputSplit: inputSplits) {
			DynamoDBQueryInputSplit split = (DynamoDBQueryInputSplit) inputSplit;
			Iterator<AttributeValue> itr = split.getRangeKeyValues().iterator();

			System.out.print(split.getRangeKeyOperator() + " ");
			System.out.print(Base64.encodeBase64String(itr.next().getB().array()) + " AND ");
			System.out.println(Base64.encodeBase64String(itr.next().getB().array()));
		}

	}
	
	@Test
	public void testCompareStrings() {
		assertEquals(0, BinarySplitter.compareBytes(new byte[] {0xA}, new byte[] {0xA}));
		assertEquals(-1, BinarySplitter.compareBytes(new byte[] {0xA}, new byte[] {0xA, 0xA}));
		assertEquals(1, BinarySplitter.compareBytes(new byte[] {0xA, 0xA}, new byte[] {0xA}));
		
		assertEquals(-1, BinarySplitter.compareBytes(new byte[] {0xA}, new byte[] {0xA, 0x0}));
		assertEquals(1, BinarySplitter.compareBytes(new byte[] {0xA, 0x0}, new byte[] {0xA}));
		
		assertEquals(-1, BinarySplitter.compareBytes(new byte[] {0xA, 0xA}, new byte[] {0xA, 0xB}));
	}
 
}
