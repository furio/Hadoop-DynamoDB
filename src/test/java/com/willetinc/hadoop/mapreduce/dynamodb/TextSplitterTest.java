package com.willetinc.hadoop.mapreduce.dynamodb;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.Test;

import com.amazonaws.services.dynamodb.model.AttributeValue;
import com.willetinc.hadoop.mapreduce.dynamodb.DynamoDBQueryInputFormat;
import com.willetinc.hadoop.mapreduce.dynamodb.TextSplitter;
import com.willetinc.hadoop.mapreduce.dynamodb.Types;

public class TextSplitterTest {

	@Test
	public void testSplitInteger() {

		final int NUM_RANGE_SPLITS = 2;
		final String VALUE = "007";
		final Types hashKeyType = Types.NUMBER;
		final AttributeValue hashKeyValue = new AttributeValue().withN(VALUE);
		final Types rangeKeyType = Types.STRING;
		final AttributeValue minRangeKeyValue =
				new AttributeValue().withS("AA");
		final AttributeValue maxRangeKeyValue =
				new AttributeValue().withS("BZ");

		Configuration conf = createMock(Configuration.class);
		TextSplitter splitter = new TextSplitter();

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
			System.out.print(itr.next() + " AND ");
			System.out.println(itr.next());
		}

	}
	
	@Test
	public void testCompareStrings() {
		assertEquals(0, TextSplitter.compareStrings("A", "A"));
		assertEquals(-1, TextSplitter.compareStrings("A", "AA"));
		assertEquals(1, TextSplitter.compareStrings("AA", "A"));
		
		assertEquals(-1, TextSplitter.compareStrings("A", "A "));
		assertEquals(1, TextSplitter.compareStrings("A ", "A"));
		
		assertEquals(-1, TextSplitter.compareStrings("AA", "AB"));
	}
 
}
