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

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.willetinc.hadoop.mapreduce.dynamodb.BinarySplitter;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBQueryInputFormat;
import com.willetinc.hadoop.mapreduce.dynamodb.Types;

public class BinarySplitterTest {

	@Test
	public void testSplitInteger() {

		final int NUM_RANGE_SPLITS = 2;
		final String VALUE = "007";
		final Types hashKeyType = Types.NUMBER;
		final AttributeValue hashKeyValue = new AttributeValue().withN(VALUE);
		final Types rangeKeyType = Types.STRING;
		final AttributeValue minRangeKeyValue =
				new AttributeValue().withB(ByteBuffer.wrap(new byte[] {0x0, 0x0}));
		final AttributeValue maxRangeKeyValue =
				new AttributeValue().withB(ByteBuffer.wrap(new byte[] {0x0, 0xF}));

		Configuration conf = createMock(Configuration.class);
		BinarySplitter splitter = new BinarySplitter();

		List<InputSplit> inputSplits = new ArrayList<InputSplit>();

		splitter.generateRangeKeySplits(
				conf,
				inputSplits,
				hashKeyType,
				hashKeyValue,
				rangeKeyType,
				minRangeKeyValue,
				maxRangeKeyValue,
				NUM_RANGE_SPLITS);

		assertEquals(2, inputSplits.size());

		
		for (InputSplit inputSplit: inputSplits) {
			DynamoDBQueryInputFormat.DynamoDBQueryInputSplit split = (DynamoDBQueryInputFormat.DynamoDBQueryInputSplit) inputSplit;
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
